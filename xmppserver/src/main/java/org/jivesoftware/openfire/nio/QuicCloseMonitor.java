/*
 * Copyright (C) 2026 Ignite Realtime Foundation. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */
package org.jivesoftware.openfire.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicConnectionPathStats;
import io.netty.handler.codec.quic.QuicConnectionStats;
import io.netty.handler.codec.quic.QuicTransportParameters;
import org.jivesoftware.openfire.session.LocalClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/** Emits the single, structured diagnostic record for the lifetime of a QUIC connection. */
public final class QuicCloseMonitor extends ChannelDuplexHandler
{
    private static final Logger Log = LoggerFactory.getLogger(QuicCloseMonitor.class);

    private final long configuredIdleTimeoutMs;
    private final long openedNanos = System.nanoTime();
    private volatile long lastActivityNanos = openedNanos;
    private volatile QuicConnectionCloseEvent peerClose;
    private volatile boolean localClose;
    private volatile QuicConnectionPathStats lastPathStats;
    private volatile long nextPathSampleNanos;

    public QuicCloseMonitor(final long configuredIdleTimeoutMs)
    {
        this.configuredIdleTimeoutMs = configuredIdleTimeoutMs;
    }

    /** Records application-stream activity on the owning connection. */
    public static void recordActivity(final Channel channel)
    {
        final QuicChannel owner = owningChannel(channel);
        if (owner != null) {
            final QuicCloseMonitor monitor = owner.pipeline().get(QuicCloseMonitor.class);
            if (monitor != null) {
                monitor.lastActivityNanos = System.nanoTime();
            }
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
    {
        lastActivityNanos = System.nanoTime();
        samplePathStats((QuicChannel) ctx.channel());
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception
    {
        lastActivityNanos = System.nanoTime();
        super.write(ctx, msg, promise);
    }

    @Override
    public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception
    {
        localClose = true;
        super.close(ctx, promise);
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception
    {
        if (evt instanceof QuicConnectionCloseEvent closeEvent) {
            peerClose = closeEvent;
            lastActivityNanos = System.nanoTime(); // the CONNECTION_CLOSE packet is activity
            samplePathStats((QuicChannel) ctx.channel());
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception
    {
        final QuicChannel channel = (QuicChannel) ctx.channel();
        final CloseDetails details = classify(channel.isTimedOut(), localClose, peerClose);
        final QuicTransportParameters peer = channel.peerTransportParameters();
        final long peerIdleMs = peer == null ? -1 : peer.maxIdleTimeout();
        final long negotiatedIdleMs = negotiatedIdleTimeout(configuredIdleTimeoutMs, peerIdleMs);
        final long activityAgeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastActivityNanos);
        final long lifetimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - openedNanos);
        final String session = sessionIdentity(channel);
        final SocketAddress remote = channel.remoteSocketAddress();

        channel.collectStats().addListener(statsFuture -> {
            final QuicConnectionStats stats = statsFuture.isSuccess() ? (QuicConnectionStats) statsFuture.getNow() : null;
            channel.collectPathStats(0).addListener(pathFuture -> {
                final QuicConnectionPathStats path = pathFuture.isSuccess()
                    ? (QuicConnectionPathStats) pathFuture.getNow() : lastPathStats;
                Log.info("event=quic_connection_closed connection_id={} session={} remote={} initiator={} close_variant={} close_code={} close_reason={} configured_idle_timeout_ms={} peer_idle_timeout_ms={} negotiated_idle_timeout_ms={} timer_fired={} last_activity_age_ms={} lifetime_ms={} rtt_ms={} packets_lost={} bytes_lost={} connection_close_sent={} connection_close_received={}",
                    channel.id().asShortText(), session, remote, details.initiator(), details.variant(), details.code(),
                    details.reason(), configuredIdleTimeoutMs, peerIdleMs, negotiatedIdleMs, details.timer(), activityAgeMs,
                    lifetimeMs, path == null ? -1 : TimeUnit.NANOSECONDS.toMillis(path.rtt()),
                    stats == null ? -1 : stats.lost(), stats == null ? -1 : stats.lostBytes(),
                    details.sent(), details.received());
            });
        });
        super.channelInactive(ctx);
    }

    private void samplePathStats(final QuicChannel channel)
    {
        final long now = System.nanoTime();
        if (now < nextPathSampleNanos) return;
        nextPathSampleNanos = now + TimeUnit.SECONDS.toNanos(1);
        channel.collectPathStats(0).addListener(future -> {
            if (future.isSuccess()) lastPathStats = (QuicConnectionPathStats) future.getNow();
        });
    }

    static CloseDetails classify(final boolean timedOut, final boolean localClose,
                                 final QuicConnectionCloseEvent peerClose)
    {
        if (timedOut) {
            return new CloseDetails("transport", "TimedOut", 0, "idle timeout", "quic_idle_timeout", false, false);
        }
        if (peerClose != null) {
            final String variant = peerClose.isApplicationClose() ? "ApplicationClosed" : "ConnectionClosed";
            return new CloseDetails("client", variant, Integer.toUnsignedLong(peerClose.error()),
                sanitizeReason(peerClose.reason()), "none", false, true);
        }
        if (localClose) {
            return new CloseDetails("server", "LocallyClosed", 0, "", "none", true, false);
        }
        // quiche exposes no stateless-reset event. A non-timeout, non-local close without a
        // CONNECTION_CLOSE frame is its observable stateless-reset signature.
        return new CloseDetails("transport", "Reset", 0, "stateless reset or transport teardown", "none", false, false);
    }

    static long negotiatedIdleTimeout(final long localMs, final long peerMs)
    {
        if (localMs <= 0) return peerMs;
        if (peerMs <= 0) return localMs;
        return Math.min(localMs, peerMs);
    }

    private static String sessionIdentity(final QuicChannel channel)
    {
        final QuicSessionStreamRouter router = QuicSessionStreamRouter.find(channel);
        final LocalClientSession session = router == null ? null : router.getSession();
        return session == null || session.getAddress() == null ? "unauthenticated" : session.getAddress().toString();
    }

    private static String sanitizeReason(final byte[] reason)
    {
        if (reason == null || reason.length == 0) return "";
        return new String(reason, StandardCharsets.UTF_8).replaceAll("[\\r\\n\\t]", " ");
    }

    private static QuicChannel owningChannel(final Channel channel)
    {
        for (Channel cursor = channel; cursor != null; cursor = cursor.parent()) {
            if (cursor instanceof QuicChannel quicChannel) return quicChannel;
        }
        return null;
    }

    record CloseDetails(String initiator, String variant, long code, String reason, String timer,
                        boolean sent, boolean received) {}
}
