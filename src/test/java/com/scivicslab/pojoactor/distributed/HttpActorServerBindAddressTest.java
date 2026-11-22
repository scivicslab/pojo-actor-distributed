package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Enumeration;
import java.net.NetworkInterface;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.scivicslab.pojoactor.core.ActorSystem;

/**
 * Which addresses an {@link HttpActorServer} accepts callers from.
 *
 * <p>Every actor in the system it is given becomes callable by anything that reaches the port,
 * so binding to every interface or to one address is the single most consequential thing about
 * a server. Both directions are asserted: that a caller on this machine gets through, and that
 * one arriving on another interface does not.
 */
@Tag("WorkflowTab_BindAddress_260906_oo01")
@Tag("HttpActorServerBinding_261123_oo01")
@DisplayName("HttpActorServer — the addresses it accepts callers from")
class HttpActorServerBindAddressTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** An address of this machine that is not the loopback one, or null when there is none. */
    private static InetAddress nonLoopbackAddress() throws Exception {
        for (Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces(); nics.hasMoreElements(); ) {
            NetworkInterface nic = nics.nextElement();
            if (!nic.isUp() || nic.isLoopback()) continue;
            for (Enumeration<InetAddress> addresses = nic.getInetAddresses(); addresses.hasMoreElements(); ) {
                InetAddress address = addresses.nextElement();
                if (!address.isLoopbackAddress() && address instanceof java.net.Inet4Address) {
                    return address;
                }
            }
        }
        return null;
    }

    private static boolean canConnect(InetAddress address, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(address, port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    void boundToLoopback_acceptsFromThisMachine() throws Exception {
        ActorSystem system = new ActorSystem("bind-loopback");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system, java.util.Set.of()), "127.0.0.1", port)) {
            server.start();

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port));
        } finally {
            system.terminate();
        }
    }

    /**
     * The point of the bind address. A server that answers on 127.0.0.1 but also on the machine's
     * LAN address has not restricted anything, and checking only the first would not have noticed.
     */
    @Test
    void boundToLoopback_refusesFromAnotherInterface() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        ActorSystem system = new ActorSystem("bind-loopback-only");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system, java.util.Set.of()), "127.0.0.1", port)) {
            server.start();

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port), "reachable on loopback");
            assertFalse(canConnect(lan, port),
                    "must not be reachable on " + lan.getHostAddress());
        } finally {
            system.terminate();
        }
    }

    /**
     * The default is this machine alone. The server has no TLS and no authentication, so listening
     * on a LAN address is something a program asks for by name, never something it gets by leaving
     * the address out.
     */
    @Test
    void withoutABindAddress_listensOnLoopbackOnly() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        ActorSystem system = new ActorSystem("bind-default");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system, java.util.Set.of()), port)) {
            server.start();

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port), "reachable on loopback");
            assertFalse(canConnect(lan, port), "must not be reachable on " + lan.getHostAddress());
        } finally {
            system.terminate();
        }
    }

    @Test
    void aNullBindAddress_isTheLoopbackDefaultToo() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        ActorSystem system = new ActorSystem("bind-null");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system, java.util.Set.of()), null, port)) {
            server.start();

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port));
            assertFalse(canConnect(lan, port), "null must not mean every interface");
        } finally {
            system.terminate();
        }
    }

    /** Hosts that must reach each other ask for it with an address; the server then says what that costs. */
    @Test
    void anExplicitNonLoopbackAddress_listensThereAndWarnsThatThereIsNoTlsAndNoAuthentication() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        java.util.List<java.util.logging.LogRecord> records = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) { records.add(r); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        java.util.logging.Logger log = java.util.logging.Logger.getLogger(HttpActorServer.class.getName());
        log.addHandler(capture);
        ActorSystem system = new ActorSystem("bind-explicit");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system, java.util.Set.of()),
                HttpActorServer.ALL_INTERFACES, port)) {
            server.start();

            assertTrue(canConnect(lan, port), "reachable on " + lan.getHostAddress());
            String warning = records.stream()
                    .filter(r -> r.getLevel() == java.util.logging.Level.WARNING)
                    .map(java.util.logging.LogRecord::getMessage).findFirst().orElse("");
            assertTrue(warning.contains("no TLS") && warning.contains("no authentication"),
                    "the start-up warning must say there is no TLS and no authentication, was: " + warning);
        } finally {
            log.removeHandler(capture);
            system.terminate();
        }
    }

    @Test
    void aLoopbackServer_logsNoWarning() throws Exception {
        java.util.List<java.util.logging.LogRecord> records = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) { records.add(r); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        java.util.logging.Logger log = java.util.logging.Logger.getLogger(HttpActorServer.class.getName());
        log.addHandler(capture);
        ActorSystem system = new ActorSystem("bind-quiet");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system, java.util.Set.of()), port)) {
            server.start();

            assertTrue(records.stream().noneMatch(r -> r.getLevel() == java.util.logging.Level.WARNING));
        } finally {
            log.removeHandler(capture);
            system.terminate();
        }
    }

    @Test
    void distributedActorSystem_startHttpServerWithoutAnAddress_isLoopbackOnly() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        int port = freePort();
        DistributedIIActorSystem system = DistributedIIActorSystem.builder().noDeduplication()
                .discovery(new OneNodeDiscovery(port))
                .build();
        try {
            system.startHttpServer(port);

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port));
            assertFalse(canConnect(lan, port), "must not be reachable on " + lan.getHostAddress());
        } finally {
            system.close();
        }
    }

    @Test
    void distributedActorSystem_passesTheBindAddressThrough() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        int port = freePort();
        DistributedIIActorSystem system = DistributedIIActorSystem.builder().noDeduplication()
                .discovery(new OneNodeDiscovery(port))
                .build();
        try {
            system.startHttpServer("127.0.0.1", port);

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port));
            assertFalse(canConnect(lan, port),
                    "must not be reachable on " + lan.getHostAddress());
        } finally {
            system.close();
        }
    }

    /** Test double: this machine, alone. Nothing here reaches the network. */
    private record OneNodeDiscovery(int port)
            implements com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery {

        @Override public String getMyNodeId() { return "me"; }

        @Override public String getMyHost() { return "127.0.0.1"; }

        @Override public int getMyPort() { return port; }

        @Override public java.util.List<NodeInfo> getAllNodes() { return java.util.List.of(); }

        @Override public boolean isApplicable() { return true; }
    }
}
