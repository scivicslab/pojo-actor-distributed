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
        try (HttpActorServer server = new HttpActorServer(system, "127.0.0.1", port)) {
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
        try (HttpActorServer server = new HttpActorServer(system, "127.0.0.1", port)) {
            server.start();

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port), "reachable on loopback");
            assertFalse(canConnect(lan, port),
                    "must not be reachable on " + lan.getHostAddress());
        } finally {
            system.terminate();
        }
    }

    /** The existing constructor keeps listening everywhere: a cluster of hosts needs that. */
    @Test
    void withoutABindAddress_listensOnEveryInterface() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        ActorSystem system = new ActorSystem("bind-wildcard");
        int port = freePort();
        try (HttpActorServer server = new HttpActorServer(system, port)) {
            server.start();

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port));
            assertTrue(canConnect(lan, port), "reachable on " + lan.getHostAddress());
        } finally {
            system.terminate();
        }
    }

    @Test
    void distributedActorSystem_passesTheBindAddressThrough() throws Exception {
        InetAddress lan = nonLoopbackAddress();
        Assumptions.assumeTrue(lan != null, "no non-loopback IPv4 address on this machine");

        ActorSystem local = new ActorSystem("bind-through");
        int port = freePort();
        DistributedActorSystem system = DistributedActorSystem.builder()
                .localActorSystem(local)
                .discovery(new OneNodeDiscovery(port))
                .build();
        try {
            system.startHttpServer("127.0.0.1", port);

            assertTrue(canConnect(InetAddress.getByName("127.0.0.1"), port));
            assertFalse(canConnect(lan, port),
                    "must not be reachable on " + lan.getHostAddress());
        } finally {
            system.close();
            local.terminate();
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
