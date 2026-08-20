/*
 * Copyright (C) 2005-2008 Jive Software, 2017-2020 Ignite Realtime Foundation. All rights reserved.
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
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.jivesoftware.util.cache.CacheSizes;
import org.jivesoftware.util.cache.Cacheable;
import org.jivesoftware.util.cache.CannotCalculateSizeException;
import org.jivesoftware.util.cache.ExternalizableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.forms.DataForm;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains identities and supported features describing client capabilities
 * for an entity.
 * 
 * @author Armando Jagucki
 *
 */
// TODO: Instances of this class should not be cached in distributed caches. The overhead of distributing data is a lot higher than recalculating the hash on every cluster node. We should remove the Externalizable interface, and turn this class into an immutable class.
public class EntityCapabilities implements Cacheable, Externalizable {

    private static final Logger Log = LoggerFactory.getLogger( EntityCapabilities.class );

    /**
     * Identities included in these entity capabilities.
     */
    private Set<String> identities = new HashSet<>();

    /**
     * Features included in these entity capabilities.
     */
    private Set<String> features = new HashSet<>();

    /**
     * XEP-0128 extended service discovery information (data forms) that were included in the disco#info response
     * that was used to populate these entity capabilities.
     *
     * Only forms that carry a FORM_TYPE field of type 'hidden' are retained here, as those are the only forms that
     * are guaranteed to be covered by the XEP-0115 §5.4 'ver' hash computation. This ensures that anything served
     * from this cache is consistent with (and verified by) the 'ver' hash under which it is stored.
     *
     * Stored as raw XML, rather than as {@link DataForm} instances, as the latter is not (easily) serializable.
     */
    private Set<String> extendedInfo = new LinkedHashSet<>();

    /**
     * Hash string that corresponds to the entity capabilities. To be
     * regenerated and used for discovering potential poisoning of entity
     * capabilities information.
     */
    private String verAttribute;

    /**
     * The hash algorithm that was used to create the hash string.
     */
    private String hashAttribute;
    
    /**
     * Adds an identity to the entity capabilities.
     * 
     * @param identity the identity
     * @return true if the entity capabilities did not already include the
     *         identity
     */
    boolean addIdentity(String identity) {
        return identities.add(identity);
    }

    /**
     * Adds a feature to the entity capabilities.
     * 
     * @param feature the feature
     * @return true if the entity capabilities did not already include the
     *         feature
     */
    boolean addFeature(String feature) {
        return features.add(feature);
    }

    /**
     * Adds a XEP-0128 extended service discovery information data form to the entity capabilities.
     *
     * @param form the data form (a XML element in the 'jabber:x:data' namespace)
     * @return true if the entity capabilities did not already include an equivalent form
     */
    boolean addExtendedInfo(Element form) {
        return extendedInfo.add(form.asXML());
    }

    /**
     * Returns the identities of the entity capabilities.
     *
     * @return all identities.
     */
    public Set<String> getIdentities()
    {
        return identities;
    }

    /**
     * Determines whether or not a given identity is included in these entity
     * capabilities.
     * 
     * @param category the category of the identity
     * @param type the type of the identity
     * @return true if identity is included, false if not
     */
    public boolean containsIdentity(String category, String type) {
        return identities.contains(category + "/" + type);
    }

    /**
     * Returns the features of the entity capabilities.
     *
     * @return all features.
     */
    public Set<String> getFeatures()
    {
        return features;
    }

    /**
     * Determines whether or not a given feature is included in these entity
     * capabilities.
     * 
     * @param feature the feature
     * @return true if feature is included, false if not
     */
    public boolean containsFeature(String feature) {
        return features.contains(feature);
    }

    /**
     * Returns the XEP-0128 extended service discovery information (data forms) that were verified as part of
     * these entity capabilities' 'ver' hash.
     *
     * @return all extended service discovery data forms (possibly empty, never null).
     */
    public Set<DataForm> getExtendedInfo()
    {
        return extendedInfo.stream()
            .map(xml -> {
                try {
                    return new DataForm(DocumentHelper.parseText(xml).getRootElement());
                } catch (DocumentException e) {
                    Log.warn("Unable to parse cached extended disco info form XML: {}", xml, e);
                    return null;
                }
            })
            .filter(java.util.Objects::nonNull)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    void setVerAttribute(String verAttribute) {
        this.verAttribute = verAttribute;
    }
    
    String getVerAttribute() {
        return this.verAttribute;
    }

    void setHashAttribute(String hashAttribute) {
        this.hashAttribute = hashAttribute;
    }

    String getHashAttribute() {
        return this.hashAttribute;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ExternalizableUtil.getInstance().readStrings(in, identities);
        ExternalizableUtil.getInstance().readStrings(in, features);
        verAttribute = ExternalizableUtil.getInstance().readSafeUTF(in);
        ExternalizableUtil.getInstance().readStrings(in, extendedInfo);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ExternalizableUtil.getInstance().writeStrings(out, identities);
        ExternalizableUtil.getInstance().writeStrings(out, features);
        ExternalizableUtil.getInstance().writeSafeUTF(out, verAttribute);
        ExternalizableUtil.getInstance().writeStrings(out, extendedInfo);
    }

    @Override
    public int getCachedSize() throws CannotCalculateSizeException {
        int size = CacheSizes.sizeOfCollection(identities);
        size += CacheSizes.sizeOfCollection(features);
        size += CacheSizes.sizeOfString(verAttribute);
        size += CacheSizes.sizeOfCollection(extendedInfo);
        return size;
    }
}
