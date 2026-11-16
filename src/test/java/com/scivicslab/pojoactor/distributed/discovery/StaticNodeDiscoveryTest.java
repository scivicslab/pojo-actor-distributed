package com.scivicslab.pojoactor.distributed.discovery;

import com.scivicslab.pojoactor.distributed.NodeInfo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** A node list that the caller configures: no environment is probed. */
class StaticNodeDiscoveryTest {

    private final NodeInfo me = new NodeInfo("a", "127.0.0.1", 28001);
    private final NodeInfo b = new NodeInfo("b", "127.0.0.1", 28002);
    private final NodeInfo c = new NodeInfo("c", "10.0.0.3", 28003);

    @Test
    void reportsMyIdentityAndEveryNode() {
        NodeDiscovery d = new StaticNodeDiscovery(me, List.of(b, c));

        assertEquals("a", d.getMyNodeId());
        assertEquals("127.0.0.1", d.getMyHost());
        assertEquals(28001, d.getMyPort());
        assertEquals(List.of("a", "b", "c"), d.getAllNodes().stream().map(NodeInfo::getNodeId).toList());
    }

    @Test
    void isAlwaysApplicable_becauseTheCallerChoseIt() {
        assertTrue(new StaticNodeDiscovery(me, List.of()).isApplicable());
    }

    @Test
    void aPeerListThatNamesMe_doesNotListMeTwice() {
        NodeDiscovery d = new StaticNodeDiscovery(me, List.of(me, b));
        assertEquals(List.of("a", "b"), d.getAllNodes().stream().map(NodeInfo::getNodeId).toList());
    }

    @Test
    void changingTheCallersListLaterDoesNotChangeTheDiscovery() {
        var peers = new java.util.ArrayList<>(List.of(b));
        NodeDiscovery d = new StaticNodeDiscovery(me, peers);
        peers.add(c);
        assertEquals(2, d.getAllNodes().size());
    }

    @Test
    void parsesPeersFromAConfigurationString() {
        List<NodeInfo> peers = StaticNodeDiscovery.parsePeers("b=127.0.0.1:28002, c=10.0.0.3:28003");

        assertEquals(2, peers.size());
        assertEquals("b", peers.get(0).getNodeId());
        assertEquals("127.0.0.1", peers.get(0).getHost());
        assertEquals(28002, peers.get(0).getPort());
        assertEquals("c", peers.get(1).getNodeId());
        assertEquals(28003, peers.get(1).getPort());
    }

    @Test
    void emptyOrBlankConfigurationMeansNoPeers() {
        assertTrue(StaticNodeDiscovery.parsePeers("").isEmpty());
        assertTrue(StaticNodeDiscovery.parsePeers("  ").isEmpty());
        assertTrue(StaticNodeDiscovery.parsePeers(null).isEmpty());
    }

    @Test
    void malformedEntryIsRejectedWithItsText() {
        for (String bad : new String[] {"b", "b=host", "b=host:notaport", "=host:1", "b=:1"}) {
            var e = assertThrows(IllegalArgumentException.class, () -> StaticNodeDiscovery.parsePeers(bad), bad);
            assertTrue(e.getMessage().contains(bad), e.getMessage());
        }
    }
}
