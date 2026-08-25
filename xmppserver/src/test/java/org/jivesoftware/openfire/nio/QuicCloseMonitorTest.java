/*
 * Copyright (C) 2026 Ignite Realtime Foundation. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package org.jivesoftware.openfire.nio;

import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.*;

class QuicCloseMonitorTest
{
    @Test
    void idleTimeoutIsTransportTimerAndSendsNoCloseFrame()
    {
        final var close = QuicCloseMonitor.classify(true, false, null);
        assertAll(
            () -> assertEquals("TimedOut", close.variant()),
            () -> assertEquals("transport", close.initiator()),
            () -> assertEquals("quic_idle_timeout", close.timer()),
            () -> assertFalse(close.sent()),
            () -> assertFalse(close.received())
        );
    }

    @Test
    void applicationCloseIsReceivedFromClientWithExactCode() throws Exception
    {
        final var close = QuicCloseMonitor.classify(false, false, closeEvent(true, 42, "done"));
        assertAll(
            () -> assertEquals("ApplicationClosed", close.variant()),
            () -> assertEquals(42, close.code()),
            () -> assertEquals("client", close.initiator()),
            () -> assertTrue(close.received()),
            () -> assertEquals("none", close.timer())
        );
    }

    @Test
    void transportCloseIsNotReportedAsIdleTimeout() throws Exception
    {
        final var close = QuicCloseMonitor.classify(false, false, closeEvent(false, 10, "protocol"));
        assertEquals("ConnectionClosed", close.variant());
        assertEquals(10, close.code());
        assertEquals("none", close.timer());
    }

    @Test
    void statelessResetHasNeitherCloseFrameNorIdleTimer()
    {
        final var close = QuicCloseMonitor.classify(false, false, null);
        assertAll(
            () -> assertEquals("Reset", close.variant()),
            () -> assertEquals("transport", close.initiator()),
            () -> assertFalse(close.sent()),
            () -> assertFalse(close.received()),
            () -> assertEquals("none", close.timer())
        );
    }

    @Test
    void localPolicyCloseIsAttributedToServer()
    {
        final var close = QuicCloseMonitor.classify(false, true, null);
        assertEquals("LocallyClosed", close.variant());
        assertEquals("server", close.initiator());
        assertTrue(close.sent());
    }

    @Test
    void networkSilenceNegotiatesTheShorterIdleTimer()
    {
        assertEquals(15_000, QuicCloseMonitor.negotiatedIdleTimeout(30_000, 15_000));
        assertNotEquals("Reset", QuicCloseMonitor.classify(true, false, null).variant());
    }

    private static QuicConnectionCloseEvent closeEvent(final boolean application, final int code,
                                                        final String reason) throws Exception
    {
        final Constructor<QuicConnectionCloseEvent> constructor = QuicConnectionCloseEvent.class
            .getDeclaredConstructor(boolean.class, int.class, byte[].class);
        constructor.setAccessible(true);
        return constructor.newInstance(application, code, reason.getBytes());
    }
}
