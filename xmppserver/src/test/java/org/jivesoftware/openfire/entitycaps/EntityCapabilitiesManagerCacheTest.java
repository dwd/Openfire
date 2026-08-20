/*
 * Copyright (C) 2026 Ignite Realtime Foundation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jivesoftware.openfire.entitycaps;

import org.dom4j.Element;
import org.dom4j.QName;
import org.jivesoftware.Fixtures;
import org.jivesoftware.openfire.IQRouter;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.util.cache.CacheFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.xmpp.forms.DataForm;
import org.xmpp.packet.IQ;
import org.xmpp.packet.JID;
import org.xmpp.packet.Presence;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test cases that verify that {@link EntityCapabilitiesManager} correctly populates (and exposes) its
 * 'ver'-hash-keyed cache, including the XEP-0128 extended service discovery information that is now retained
 * alongside identities and features.
 *
 * @see <a href="https://xmpp.org/extensions/xep-0115.html">XEP-0115: Entity Capabilities</a>
 * @see <a href="https://xmpp.org/extensions/xep-0128.html">XEP-0128: Service Discovery Extensions</a>
 */
public class EntityCapabilitiesManagerCacheTest {

    private XMPPServer xmppServer;
    private IQRouter iqRouter;
    private EntityCapabilitiesManager manager;

    @BeforeEach
    public void setUp() throws Exception {
        CacheFactory.initialize();
        Fixtures.reconfigureOpenfireHome();
        Fixtures.disableDatabasePersistence();

        xmppServer = Fixtures.mockXMPPServer();
        iqRouter = xmppServer.getIQRouter();
        XMPPServer.setInstance(xmppServer);

        manager = new EntityCapabilitiesManager();
        manager.initialize(xmppServer);
    }

    @AfterEach
    public void tearDown() {
        manager.destroy();
        XMPPServer.setInstance(null);
    }

    /**
     * Drives the manager through a realistic disco#info round-trip: a presence stanza advertising an
     * (initially unrecognized) 'ver' hash is processed, causing the manager to dispatch a disco#info query. That
     * query is then answered with a response that yields (by construction) the same 'ver' hash, causing the
     * manager to cache the response's identities, features, and (hidden FORM_TYPE) extended forms.
     *
     * @return the 'ver' hash under which the capabilities got cached.
     */
    private String populateCacheViaRoundTrip(JID from, List<Element> identities, List<String> features, List<Element> forms) {
        final IQ template = new IQ(IQ.Type.result);
        template.setFrom(from);
        template.setTo(new JID(Fixtures.XMPP_DOMAIN));
        final Element query = template.setChildElement("query", "http://jabber.org/protocol/disco#info");
        identities.forEach(identity -> query.add(identity.createCopy()));
        features.forEach(feature -> query.addElement("feature").addAttribute("var", feature));
        forms.forEach(form -> query.add(form.createCopy()));

        final String verHash = EntityCapabilitiesManager.generateVerHash(template, "sha-1");

        final Presence presence = new Presence();
        presence.setFrom(from);
        final Element caps = presence.getElement().addElement("c", "http://jabber.org/protocol/caps");
        caps.addAttribute("hash", "sha-1");
        caps.addAttribute("node", "http://example.org/client");
        caps.addAttribute("ver", verHash);

        manager.process(presence);

        final ArgumentCaptor<IQ> captor = ArgumentCaptor.forClass(IQ.class);
        verify(iqRouter, atLeastOnce()).route(captor.capture());
        final IQ outboundQuery = captor.getAllValues().stream()
            .filter(iq -> from.equals(iq.getTo()))
            .reduce((first, second) -> second)
            .orElseThrow();

        final IQ result = new IQ(IQ.Type.result);
        result.setID(outboundQuery.getID());
        result.setFrom(from);
        result.setTo(outboundQuery.getFrom());
        result.setChildElement(query.createCopy());

        manager.receivedAnswer(result);

        return verHash;
    }

    private static Element identity(String category, String type, String name) {
        final Element identity = org.dom4j.DocumentHelper.createElement("identity");
        identity.addAttribute("category", category);
        identity.addAttribute("type", type);
        identity.addAttribute("name", name);
        return identity;
    }

    private static Element hiddenForm(String formType, String field, String value) {
        final Element form = org.dom4j.DocumentHelper.createElement(QName.get("x", "jabber:x:data"));
        form.addAttribute("type", "result");
        final Element ft = form.addElement("field");
        ft.addAttribute("var", "FORM_TYPE");
        ft.addAttribute("type", "hidden");
        ft.addElement("value").setText(formType);
        final Element f = form.addElement("field");
        f.addAttribute("var", field);
        f.addElement("value").setText(value);
        return form;
    }

    @Test
    public void testUnknownVerHashReturnsNull() {
        assertNull(manager.getEntityCapabilitiesByVerHash("this-hash-does-not-exist"));
    }

    @Test
    public void testKnownVerHashIsCachedWithIdentitiesAndFeatures() {
        final JID from = new JID("client@example.org/res");
        final String verHash = populateCacheViaRoundTrip(
            from,
            List.of(identity("client", "pc", "TestClient")),
            List.of("http://jabber.org/protocol/caps", "http://jabber.org/protocol/disco#info"),
            List.of());

        final EntityCapabilities cached = manager.getEntityCapabilitiesByVerHash(verHash);
        assertNotNull(cached);
        assertTrue(cached.getIdentities().stream().anyMatch(identity -> identity.startsWith("client/pc/")));
        assertTrue(cached.containsFeature("http://jabber.org/protocol/caps"));
        assertTrue(cached.containsFeature("http://jabber.org/protocol/disco#info"));
        assertTrue(cached.getExtendedInfo().isEmpty(), "No extended forms were provided, so none should be cached.");
    }

    @Test
    public void testKnownVerHashCachesHiddenFormTypeExtendedForm() {
        final JID from = new JID("client2@example.org/res");
        final String verHash = populateCacheViaRoundTrip(
            from,
            List.of(identity("client", "pc", "TestClient")),
            List.of("http://jabber.org/protocol/caps"),
            List.of(hiddenForm("urn:xmpp:dataforms:openfire-unittest", "custom-field", "custom-value")));

        final EntityCapabilities cached = manager.getEntityCapabilitiesByVerHash(verHash);
        assertNotNull(cached);

        final java.util.Set<DataForm> forms = cached.getExtendedInfo();
        assertEquals(1, forms.size());
        final DataForm form = forms.iterator().next();
        assertEquals("urn:xmpp:dataforms:openfire-unittest", form.getField("FORM_TYPE").getFirstValue());
        assertEquals("custom-value", form.getField("custom-field").getFirstValue());
    }

    @Test
    public void testNonHiddenFormTypeIsNotCached() {
        // A form whose FORM_TYPE field is not of type 'hidden' is excluded from the 'ver' hash computation (per
        // XEP-0115 §5.4 item 3f), and therefore must also not be retained in the cache: it wasn't verified by the
        // hash, so serving it from the cache in the future would not be safe.
        final Element nonHiddenForm = org.dom4j.DocumentHelper.createElement(QName.get("x", "jabber:x:data"));
        nonHiddenForm.addAttribute("type", "result");
        final Element ft = nonHiddenForm.addElement("field");
        ft.addAttribute("var", "FORM_TYPE");
        ft.addAttribute("type", "text-single"); // NOT hidden
        ft.addElement("value").setText("urn:xmpp:dataforms:openfire-unittest-nonhidden");

        final JID from = new JID("client3@example.org/res");
        final String verHash = populateCacheViaRoundTrip(
            from,
            List.of(identity("client", "pc", "TestClient")),
            List.of("http://jabber.org/protocol/caps"),
            List.of(nonHiddenForm));

        final EntityCapabilities cached = manager.getEntityCapabilitiesByVerHash(verHash);
        assertNotNull(cached);
        assertTrue(cached.getExtendedInfo().isEmpty(), "A form without a hidden FORM_TYPE must not be cached.");
    }
}
