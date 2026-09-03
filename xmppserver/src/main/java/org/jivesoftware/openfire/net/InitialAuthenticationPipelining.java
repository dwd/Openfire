/*
 * Copyright (C) 2026 Ignite Realtime Foundation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.jivesoftware.openfire.net;

import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.QName;
import org.jivesoftware.openfire.session.LocalSession;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * Implements the configuration-version part of XEP-0509 Initial Authentication Pipelining.
 */
public final class InitialAuthenticationPipelining
{
    private static final String ADVERTISED_CONFIG_VERSION = "IAP.config-version";

    private InitialAuthenticationPipelining() {
    }

    /**
     * Appends an IAP configuration version that covers every feature assembled so far.
     * This must be invoked last while assembling pre-authentication stream features.
     */
    public static void appendConfigVersion(@Nonnull final LocalSession session, @Nonnull final List<Element> features)
    {
        if (!SASLAuthentication.ENABLE_IAP.getValue()
            || session.isAuthenticated()
            || SASLAuthentication.checkSASL2Permitted(session).isPresent()) {
            session.removeSessionData(ADVERTISED_CONFIG_VERSION);
            return;
        }

        final String value = SASLAuthentication.generateConfigVersion(features);
        final Element configVersion = DocumentHelper.createElement(QName.get("config-version", SASLAuthentication.IAP_NAMESPACE));
        configVersion.addAttribute("value", value);
        features.add(configVersion);
        session.setSessionData(ADVERTISED_CONFIG_VERSION, value);
    }

    /**
     * Returns whether an IAP version supplied with a SASL2 authentication request matches the version advertised on
     * this stream. An absent version is not an IAP request and is accepted for backwards compatibility.
     */
    static boolean isConfigVersionValid(@Nonnull final LocalSession session, @Nonnull final Element authenticate)
    {
        if (!SASLAuthentication.ENABLE_IAP.getValue()) {
            return true;
        }

        final Element supplied = authenticate.element(QName.get("config-version", SASLAuthentication.IAP_NAMESPACE));
        if (supplied == null) {
            return true;
        }

        final String advertised = Optional.ofNullable(session.getSessionData(ADVERTISED_CONFIG_VERSION))
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .orElse(null);
        return advertised != null && advertised.equals(supplied.attributeValue("value"));
    }

    static void sendConfigVersionMismatch(@Nonnull final LocalSession session)
    {
        final Element failure = DocumentHelper.createElement(QName.get("failure", SASLAuthentication.SASL2_NAMESPACE));
        failure.addElement("aborted", SASLAuthentication.SASL_NAMESPACE);
        failure.addElement("config-version-mismatch", SASLAuthentication.IAP_NAMESPACE);
        session.deliverRawText(failure.asXML());
    }
}
