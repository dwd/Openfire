/**
 * 
 */
package org.jivesoftware.openfire.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jivesoftware.openfire.PacketException;
import org.jivesoftware.openfire.RoutableChannelHandler;
import org.jivesoftware.openfire.RoutingTable;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.openfire.auth.UnauthorizedException;
import org.jivesoftware.openfire.session.LocalOutgoingServerSession;
import org.jivesoftware.openfire.session.ServerSession;
import org.jivesoftware.openfire.spi.RoutingTableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.IQ;
import org.xmpp.packet.JID;
import org.xmpp.packet.Message;
import org.xmpp.packet.Packet;
import org.xmpp.packet.PacketError;
import org.xmpp.packet.Presence;

/**
 * @author dwd
 *
 */
public class LocalOutgoingServerProxy implements RoutableChannelHandler {
    private static final Logger log = LoggerFactory.getLogger(LocalOutgoingServerProxy.class);
    private JID domain;
    private ServerSession session;
    private Queue<Packet> packets;
    private ExecutorService pool = Executors.newFixedThreadPool(25);
    private long failureTimestamp = -1;
    private boolean isTrying;
    
    public LocalOutgoingServerProxy(final JID domain) {
        this.domain = domain;
        this.session = null;
        this.packets = null;
    }

    public LocalOutgoingServerProxy(final String domain) {
        this.domain = new JID(domain);
        this.session = null;
        this.packets = null;
    }

    public LocalOutgoingServerProxy(final JID domain, ServerSession session) {
        this.domain = domain;
        this.session = null;
        this.packets = null;
    }

    public LocalOutgoingServerProxy(final String domain, ServerSession session) {
        this.domain = new JID(domain);
        this.session = null;
        this.packets = null;
    }

    /* (non-Javadoc)
     * @see org.jivesoftware.openfire.ChannelHandler#process(org.xmpp.packet.Packet)
     */
    @Override
    public synchronized void process(final Packet packet) throws UnauthorizedException,
            PacketException {
        if (this.session != null) {
            this.session.process(packet);
        } else {
            if (packets == null) {
                packets = new LinkedBlockingQueue<Packet>();
            }
            if (isTrying == false) {
                packets.add(packet);
                if ((failureTimestamp == -1) || ((System.currentTimeMillis() - failureTimestamp) >= 5000)) {
                    isTrying = true;
                    log.info("Spinning up new session to {}", domain.toString());
                    pool.execute(new Runnable() {
                        public void run() {
                            try {
                                ServerSession s = LocalOutgoingServerSession.authenticateDomain(packet.getFrom().getDomain(), packet.getTo().getDomain()); // Long-running.
                                if (s != null) {
                                    sessionReady(s);
                                } else {
                                    sessionFailed();
                                }
                            } catch(Exception e) {
                                sessionFailed();
                            }
                            return;
                        }
                    });
                } else {
                    sessionFailed();
                }
            } else {
                // Session creation in progress.
                packets.add(packet);
            }
        }
    }
    
    protected synchronized void sessionReady(ServerSession session) {
        isTrying = false;
        log.info("Spun up new session to {}", domain.toString());
        this.session = session;
        while (!this.packets.isEmpty()) {
            Packet packet = this.packets.remove();
            this.session.process(packet);
        }
        this.packets = null;
        log.info("Done.");
    }
    
    protected synchronized void sessionFailed() {
        isTrying = false;
        log.info("Failed to spin up new session to {}", domain.toString());
        while (!this.packets.isEmpty()) {
            Packet packet = this.packets.remove();
            this.returnErrorToSender(packet);
        }
        this.packets = null;
    }

    private void returnErrorToSender(Packet packet) {
        XMPPServer server = XMPPServer.getInstance();
        RoutingTable routingTable = server.getRoutingTable();
        JID from = packet.getFrom();
        JID to = packet.getTo();
        if (!server.isLocal(from) && !XMPPServer.getInstance().matchesComponent(from) &&
                !server.isLocal(to) && !XMPPServer.getInstance().matchesComponent(to)) {
            // Do nothing since the sender and receiver of the packet that failed to reach a remote
            // server are not local users. This prevents endless loops if the FROM or TO address
            // are non-existen addresses
            return;
        }

        // TODO Send correct error condition: timeout or not_found depending on the real error
        try {
            if (packet instanceof IQ) {
                IQ reply = new IQ();
                reply.setID(packet.getID());
                reply.setTo(from);
                reply.setFrom(to);
                reply.setChildElement(((IQ) packet).getChildElement().createCopy());
                reply.setError(PacketError.Condition.remote_server_not_found);
                routingTable.routePacket(reply.getTo(), reply, true);
            }
            else if (packet instanceof Presence) {
                    // workaround for OF-23. "undo" the 'setFrom' to a bare JID 
                    // by sending the error to all available resources.
                    final List<JID> routes = new ArrayList<JID>(); 
                    if (from.getResource() == null || from.getResource().trim().length() == 0) {
                    routes.addAll(routingTable.getRoutes(from, null));
                } else {
                    routes.add(from);
                }
                    
                    for (JID route : routes) {
                        Presence reply = new Presence();
                        reply.setID(packet.getID());
                        reply.setTo(route);
                        reply.setFrom(to);
                        reply.setError(PacketError.Condition.remote_server_not_found);
                        routingTable.routePacket(reply.getTo(), reply, true);
                    }
            }
            else if (packet instanceof Message) {
                Message reply = new Message();
                reply.setID(packet.getID());
                reply.setTo(from);
                reply.setFrom(to);
                reply.setType(((Message)packet).getType());
                reply.setThread(((Message)packet).getThread());
                reply.setError(PacketError.Condition.remote_server_not_found);
                routingTable.routePacket(reply.getTo(), reply, true);
            }
        }
        catch (Exception e) {
            log.warn("Error returning error to sender. Original packet: " + packet, e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.jivesoftware.openfire.RoutableChannelHandler#getAddress()
     */
    @Override
    public JID getAddress() {
        return this.domain;
    }

    public ServerSession getSession() {
        return this.session;
    }
}
