package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery;
import com.scivicslab.pojoactor.distributed.transport.StubTransport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DistributedActorSystem builder and RemoteActorRef wiring.
 * Uses stub transport and discovery — no Kafka or network required.
 */
@Tag("S_load.03")
class DistributedActorSystemTest {

    /** Stub NodeDiscovery that returns two fixed nodes. */
    static class StubDiscovery implements NodeDiscovery {
        private final NodeInfo myNode;
        private final List<NodeInfo> allNodes;

        StubDiscovery(NodeInfo myNode, NodeInfo other) {
            this.myNode = myNode;
            this.allNodes = List.of(myNode, other);
        }

        @Override public String getMyNodeId() { return myNode.getNodeId(); }
        @Override public String getMyHost()   { return myNode.getHost(); }
        @Override public int    getMyPort()   { return myNode.getPort(); }
        @Override public List<NodeInfo> getAllNodes() { return allNodes; }
        @Override public boolean isApplicable() { return true; }
    }

    private final NodeInfo nodeA = new NodeInfo("node-a", "localhost", 8081);
    private final NodeInfo nodeB = new NodeInfo("node-b", "localhost", 8082);

    @Test
    void builder_shouldWireTransportIntoRemoteActorRef() {
        StubTransport stub = new StubTransport();
        stub.presetResult = new ActionResult(true, "pong");

        DistributedActorSystem system = DistributedActorSystem.builder()
                .localActorSystem(new ActorSystem("test"))
                .transport(stub)
                .discovery(new StubDiscovery(nodeA, nodeB))
                .build();

        RemoteActorRef ref = system.remoteActorOf(nodeB, "ping-actor");
        ActionResult result = ref.callByActionName("ping", "");

        assertTrue(result.isSuccess());
        assertEquals("pong", result.getResult());
        assertEquals(1, stub.sent.size());
        assertEquals("ping-actor", stub.sent.get(0).getActorName());
        assertEquals("ping", stub.sent.get(0).getActionName());

        system.close();
    }

    @Test
    void builder_shouldExposeDiscoveredNodes() {
        DistributedActorSystem system = DistributedActorSystem.builder()
                .localActorSystem(new ActorSystem("test"))
                .transport(new StubTransport())
                .discovery(new StubDiscovery(nodeA, nodeB))
                .build();

        List<NodeInfo> nodes = system.getNodes();
        assertEquals(2, nodes.size());
        assertTrue(nodes.stream().anyMatch(n -> n.getNodeId().equals("node-a")));
        assertTrue(nodes.stream().anyMatch(n -> n.getNodeId().equals("node-b")));

        system.close();
    }

    @Test
    void builder_shouldRejectMissingActorSystem() {
        assertThrows(IllegalStateException.class, () ->
                DistributedActorSystem.builder()
                        .transport(new StubTransport())
                        .discovery(new StubDiscovery(nodeA, nodeB))
                        .build());
    }

    @Test
    void myNode_shouldMatchDiscovery() {
        DistributedActorSystem system = DistributedActorSystem.builder()
                .localActorSystem(new ActorSystem("test"))
                .transport(new StubTransport())
                .discovery(new StubDiscovery(nodeA, nodeB))
                .build();

        assertEquals("node-a", system.getMyNode().getNodeId());
        assertEquals("localhost", system.getMyNode().getHost());
        assertEquals(8081, system.getMyNode().getPort());

        system.close();
    }
}
