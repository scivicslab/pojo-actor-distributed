package com.scivicslab.pojoactor.distributed.transport;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;

import java.time.Duration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for TransportLayer implementations using a stub transport.
 * Verifies the interface semantics without requiring real network connections.
 */
@Tag("S_load.02")
public class TransportLayerContractTest {

    private final NodeInfo nodeA = new NodeInfo("node-a", "localhost", 8081);
    private final NodeInfo nodeB = new NodeInfo("node-b", "localhost", 8082);

    @Test
    void send_shouldRecordMessage() {
        StubTransport transport = new StubTransport();
        ActorMessage msg = new ActorMessage("worker", "process", "hello");

        transport.send(nodeB, msg);

        assertEquals(1, transport.sent.size());
        assertEquals("worker", transport.sent.get(0).getActorName());
        assertEquals("process", transport.sent.get(0).getActionName());
        assertEquals("hello", transport.sent.get(0).getArgs());
    }

    @Test
    void sendAndWait_shouldReturnPresetResult() {
        StubTransport transport = new StubTransport();
        transport.presetResult = new ActionResult(true, "42");
        ActorMessage msg = new ActorMessage("calc", "add", "40,2");

        ActionResult result = transport.sendAndWait(nodeB, msg, Duration.ofSeconds(5));

        assertTrue(result.isSuccess());
        assertEquals("42", result.getResult());
    }

    @Test
    void sendAndWait_shouldPropagateFailure() {
        StubTransport transport = new StubTransport();
        transport.presetResult = new ActionResult(false, "actor not found");
        ActorMessage msg = new ActorMessage("unknown", "doThing", "");

        ActionResult result = transport.sendAndWait(nodeB, msg, Duration.ofSeconds(5));

        assertFalse(result.isSuccess());
        assertEquals("actor not found", result.getResult());
    }

    @Test
    void send_shouldNotBlockCaller() {
        // fire-and-forget: send() must not throw even if transport is closed
        StubTransport transport = new StubTransport();
        transport.close();
        // After close, send should still not throw (stub behavior)
        assertDoesNotThrow(() ->
                transport.send(nodeA, new ActorMessage("actor", "action", "args")));
    }
}
