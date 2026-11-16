package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery;
import com.scivicslab.pojoactor.distributed.transport.StubTransport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DistributedIIActorSystem builder and RemoteActorRef wiring.
 * Uses stub transport and discovery — no Kafka or network required.
 */
@Tag("S_load.03")
class DistributedIIActorSystemTest {

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

        DistributedIIActorSystem system = DistributedIIActorSystem.builder()
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
        DistributedIIActorSystem system = DistributedIIActorSystem.builder()
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
    void mergedSystemIsAnIIActorSystem() {
        DistributedIIActorSystem system = DistributedIIActorSystem.builder()
                .transport(new StubTransport())
                .discovery(new StubDiscovery(nodeA, nodeB))
                .build();

        assertInstanceOf(com.scivicslab.turingworkflow.workflow.IIActorSystem.class, system);
        assertEquals("node-a", system.getMyNode().getNodeId());

        system.close();
    }

    @Test
    void peersFromDiscoveryAreRegisteredWithoutMe() {
        DistributedIIActorSystem system = DistributedIIActorSystem.builder()
                .transport(new StubTransport())
                .discovery(new StubDiscovery(nodeA, nodeB))
                .build();

        RemoteActorRef ref = system.getRemoteActor("node-b", "x");
        assertEquals("node-b", ref.getNodeInfo().getNodeId());
        assertThrows(IllegalArgumentException.class, () -> system.getRemoteActor("node-a", "x"));

        system.close();
    }

    @Test
    void myNode_shouldMatchDiscovery() {
        DistributedIIActorSystem system = DistributedIIActorSystem.builder()
                .transport(new StubTransport())
                .discovery(new StubDiscovery(nodeA, nodeB))
                .build();

        assertEquals("node-a", system.getMyNode().getNodeId());
        assertEquals("localhost", system.getMyNode().getHost());
        assertEquals(8081, system.getMyNode().getPort());

        system.close();
    }
}
