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
package org.jivesoftware.openfire.disco;

import org.dom4j.Element;
import org.dom4j.QName;
import org.jivesoftware.Fixtures;
import org.jivesoftware.openfire.IQRouter;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.openfire.entitycaps.EntityCapabilitiesManager;
import org.jivesoftware.util.cache.CacheFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jivesoftware.openfire.XMPPServerInfo;
import org.jivesoftware.util.Version;
import org.mockito.ArgumentCaptor;
import org.xmpp.packet.IQ;
import org.xmpp.packet.JID;
import org.xmpp.packet.PacketError;
import org.xmpp.packet.Presence;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for the opt-in (default off) short-circuit in {@link IQDiscoInfoHandler} that answers XEP-0115 caps
 * verification-style disco#info requests (i.e. a request whose 'node' attribute is of the form
 * "&lt;uri&gt;#&lt;ver&gt;") directly from the {@link EntityCapabilitiesManager} cache, when the 'ver' hash is
 * already known.
 *
 * @see <a href="https://xmpp.org/extensions/xep-0115.html">XEP-0115: Entity Capabilities</a>
 * @see <a href="https://xmpp.org/extensions/xep-0128.html">XEP-0128: Service Discovery Extensions</a>
 * @see <a href="https://xmpp.org/extensions/xep-0030.html">XEP-0030: Service Discovery</a>
 */
public class IQDiscoInfoHandlerCapsCacheShortCircuitTest {

    private static final String CAPS_NODE = "http://example.org/client";

    private XMPPServer xmppServer;
    private IQRouter iqRouter;
    private EntityCapabilitiesManager entityCapabilitiesManager;
    private IQDiscoInfoHandler handler;

    @BeforeEach
    public void setUp() throws Exception {
        CacheFactory.initialize();
        Fixtures.reconfigureOpenfireHome();
        Fixtures.disableDatabasePersistence();

        xmppServer = Fixtures.mockXMPPServer();
        iqRouter = xmppServer.getIQRouter();

        entityCapabilitiesManager = new EntityCapabilitiesManager();
        entityCapabilitiesManager.initialize(xmppServer);
        doReturn(entityCapabilitiesManager).when(xmppServer).getEntityCapabilitiesManager();

        XMPPServer.setInstance(xmppServer);

        handler = new IQDiscoInfoHandler();
        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(false);
    }

    @AfterEach
    public void tearDown() {
        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(false);
        entityCapabilitiesManager.destroy();
        XMPPServer.setInstance(null);
    }

    /**
     * Drives {@link EntityCapabilitiesManager} through a realistic disco#info round-trip (presence with caps,
     * followed by a matching, verified disco#info response) to populate its cache, exactly as would happen in
     * production when a real caps-advertising entity is first encountered.
     *
     * @return the 'ver' hash under which the capabilities got cached.
     */
    private String populateCache(JID from, List<Element> identities, List<String> features, List<Element> forms) {
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
        caps.addAttribute("node", CAPS_NODE);
        caps.addAttribute("ver", verHash);

        entityCapabilitiesManager.process(presence);

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

        entityCapabilitiesManager.receivedAnswer(result);

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

    private IQ discoInfoRequest(JID from, JID to, String node) {
        final IQ request = new IQ(IQ.Type.get);
        request.setFrom(from);
        request.setTo(to);
        final Element query = request.setChildElement("query", "http://jabber.org/protocol/disco#info");
        if (node != null) {
            query.addAttribute("node", node);
        }
        return request;
    }

    @Test
    public void testKnownCapsHashIsServedFromCacheWhenEnabled() {
        final JID capsEntity = new JID("known@example.org/res");
        final String verHash = populateCache(
            capsEntity,
            List.of(identity("client", "pc", "TestClient")),
            List.of("http://jabber.org/protocol/caps", "http://jabber.org/protocol/disco#info"),
            List.of());

        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(true);

        // Query is sent to an entity that has no registered DiscoInfoProvider at all: a cache hit must be served
        // regardless (this proves the answer really came from the cache, not from a live provider).
        final IQ request = discoInfoRequest(new JID("requester@example.org"), new JID("someone@unregistered.example.org"), CAPS_NODE + "#" + verHash);

        final IQ reply = handler.handleIQ(request);

        assertNotNull(reply);
        assertNull(reply.getError());
        final Element query = reply.getChildElement();
        assertNotNull(query);
        assertEquals(CAPS_NODE + "#" + verHash, query.attributeValue("node"));

        final Element identityElement = query.element("identity");
        assertNotNull(identityElement);
        assertEquals("client", identityElement.attributeValue("category"));
        assertEquals("pc", identityElement.attributeValue("type"));
        assertEquals("TestClient", identityElement.attributeValue("name"));

        final List<Element> featureElements = query.elements("feature");
        final List<String> featureVars = featureElements.stream().map(e -> e.attributeValue("var")).toList();
        assertTrue(featureVars.contains("http://jabber.org/protocol/caps"));
        assertTrue(featureVars.contains("http://jabber.org/protocol/disco#info"));

        assertTrue(query.elements(QName.get("x", "jabber:x:data")).isEmpty(), "No extended forms were cached, so none should be present.");
    }

    @Test
    public void testKnownCapsHashWithExtendedFormIsServedFromCacheWhenEnabled() {
        final JID capsEntity = new JID("known-with-form@example.org/res");
        final String verHash = populateCache(
            capsEntity,
            List.of(identity("client", "pc", "TestClient")),
            List.of("http://jabber.org/protocol/caps"),
            List.of(hiddenForm("urn:xmpp:dataforms:openfire-unittest", "custom-field", "custom-value")));

        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(true);

        final IQ request = discoInfoRequest(new JID("requester@example.org"), new JID("someone@unregistered.example.org"), CAPS_NODE + "#" + verHash);
        final IQ reply = handler.handleIQ(request);

        assertNotNull(reply);
        assertNull(reply.getError());
        final Element query = reply.getChildElement();
        final List<Element> forms = query.elements(QName.get("x", "jabber:x:data"));
        assertEquals(1, forms.size());
        final Element form = forms.get(0);
        boolean foundFormType = false;
        boolean foundCustomField = false;
        for (Object o : form.elements("field")) {
            final Element field = (Element) o;
            if ("FORM_TYPE".equals(field.attributeValue("var"))) {
                assertEquals("urn:xmpp:dataforms:openfire-unittest", field.element("value").getText());
                foundFormType = true;
            } else if ("custom-field".equals(field.attributeValue("var"))) {
                assertEquals("custom-value", field.element("value").getText());
                foundCustomField = true;
            }
        }
        assertTrue(foundFormType);
        assertTrue(foundCustomField);
    }

    @Test
    public void testUnknownCapsHashFallsBackToNormalHandling() {
        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(true);

        // A syntactically valid caps-style node, but its 'ver' hash was never cached.
        final IQ request = discoInfoRequest(new JID("requester@example.org"), new JID("someone@unregistered.example.org"), CAPS_NODE + "#unknown-ver-hash-1234");
        final IQ reply = handler.handleIQ(request);

        assertNotNull(reply);
        // No DiscoInfoProvider is registered for "unregistered.example.org", so falling back to normal handling
        // must result in a "not found" error, proving that the cache was not (and could not be) consulted.
        assertNotNull(reply.getError());
        assertEquals(PacketError.Condition.item_not_found, reply.getError().getCondition());
    }

    @Test
    public void testShortCircuitDisabledByDefaultEvenWithKnownHash() {
        final JID capsEntity = new JID("known-but-disabled@example.org/res");
        final String verHash = populateCache(
            capsEntity,
            List.of(identity("client", "pc", "TestClient")),
            List.of("http://jabber.org/protocol/caps"),
            List.of());

        // Feature flag intentionally left at its default (false/off).
        final IQ request = discoInfoRequest(new JID("requester@example.org"), new JID("someone@unregistered.example.org"), CAPS_NODE + "#" + verHash);
        final IQ reply = handler.handleIQ(request);

        assertNotNull(reply);
        assertNotNull(reply.getError(), "With the feature disabled (the default), a known 'ver' hash must not be served from the cache.");
        assertEquals(PacketError.Condition.item_not_found, reply.getError().getCondition());
    }

    @Test
    public void testNonCapsDiscoInfoRequestIsUnaffected() {
        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(true);

        // Provide at least one admin, and a mocked software version, to avoid NPEs from the built-in
        // ExtendedDiscoInfoProviders that are triggered by the normal (non-short-circuited) handling path.
        doReturn(java.util.Set.of(new JID("admin@" + Fixtures.XMPP_DOMAIN))).when(xmppServer).getAdmins();
        final XMPPServerInfo serverInfo = xmppServer.getServerInfo();
        final Version version = mock(Version.class);
        when(version.getVersionString()).thenReturn("5.1.0-TEST");
        when(serverInfo.getVersion()).thenReturn(version);

        handler.initialize(xmppServer);
        handler.start();

        // A plain disco#info request without a 'node' attribute, sent to the server itself: must be handled
        // normally (through the registered server DiscoInfoProvider), unaffected by the new feature.
        final IQ request = discoInfoRequest(new JID("requester@example.org"), new JID(Fixtures.XMPP_DOMAIN), null);
        final IQ reply = handler.handleIQ(request);

        assertNotNull(reply);
        assertNull(reply.getError());
        final Element query = reply.getChildElement();
        assertNotNull(query);
        final List<Element> features = query.elements("feature");
        assertTrue(features.stream().anyMatch(f -> IQDiscoInfoHandler.NAMESPACE_DISCO_INFO.equals(f.attributeValue("var"))));
    }

    @Test
    public void testCapsStyleNodeWithoutHashSuffixIsUnaffected() {
        IQDiscoInfoHandler.ENTITY_CAPS_CACHE_SHORT_CIRCUIT.setValue(true);

        // A 'node' that contains no '#' at all is not shaped like a caps verification request, and must not be
        // treated as one.
        final IQ request = discoInfoRequest(new JID("requester@example.org"), new JID("someone@unregistered.example.org"), CAPS_NODE);
        final IQ reply = handler.handleIQ(request);

        assertNotNull(reply);
        assertNotNull(reply.getError());
        assertEquals(PacketError.Condition.item_not_found, reply.getError().getCondition());
    }
}
