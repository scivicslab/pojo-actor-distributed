package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.RemoteActorRef;
import com.scivicslab.pojoactor.distributed.transport.TransportLayer;
import com.scivicslab.turingworkflow.workflow.IIActorRef;
import com.scivicslab.turingworkflow.workflow.IIActorSystem;
import com.scivicslab.turingworkflow.workflow.CalcActor;

/**
 * Standing in, inside this actor system, for an actor that lives in another process.
 *
 * <p>Nothing here touches the network: the transport is a recorder, so what is asserted is what
 * the proxy sends and what it does with the answer, not that HTTP works.
 */
@Tag("RemoteChildActor_Proxy_260906_oo01")
@DisplayName("RemoteActorIIAR — an actor in another process, called by name")
class RemoteActorIIARTest {

    /** Records what it was asked to send and answers with whatever it was told to answer. */
    static class RecordingTransport implements TransportLayer {
        final List<ActorMessage> sent = new ArrayList<>();
        ActionResult answer = new ActionResult(true, "ok");
        RuntimeException failure;

        @Override public void send(NodeInfo target, ActorMessage message) { sent.add(message); }

        @Override
        public ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout) {
            sent.add(message);
            if (failure != null) throw failure;
            return answer;
        }

        @Override public void close() { }
    }

    private static RemoteActorIIAR proxy(String localName, String remoteName,
            RecordingTransport transport, IIActorSystem system) {
        NodeInfo node = new NodeInfo("child", "127.0.0.1", 28116);
        return new RemoteActorIIAR(localName, new RemoteActorRef(remoteName, node, transport), system);
    }

    @Test
    void forwardsTheActionNameAndArgumentsUnchanged() {
        IIActorSystem system = new IIActorSystem("parent");
        RecordingTransport transport = new RecordingTransport();
        try {
            RemoteActorIIAR remote =
                    proxy("chat-ui:project1/chat-01.chat", "project1/chat-01.chat", transport, system);

            ActionResult result = remote.callByActionName("sendPrompt", "\"read this\"");

            assertEquals(1, transport.sent.size());
            assertEquals("project1/chat-01.chat", transport.sent.get(0).getActorName());
            assertEquals("sendPrompt", transport.sent.get(0).getActionName());
            assertEquals("\"read this\"", transport.sent.get(0).getArgs());
            assertTrue(result.isSuccess());
        } finally {
            system.terminate();
        }
    }

    /**
     * The proxy knows no action names — which actions exist is the child's business, and a list
     * held here would go stale the moment the child gains one.
     */
    @Test
    void doesNotDecideWhichActionsExist() {
        IIActorSystem system = new IIActorSystem("parent");
        RecordingTransport transport = new RecordingTransport();
        transport.answer = new ActionResult(false, "Unknown action: nonesuch");
        try {
            RemoteActorIIAR remote = proxy("chat-ui:x", "x", transport, system);

            ActionResult result = remote.callByActionName("nonesuch", "");

            assertEquals(1, transport.sent.size(), "the call must reach the child, not be refused here");
            assertFalse(result.isSuccess());
        } finally {
            system.terminate();
        }
    }

    /** A child that is not running must fail one action, not end the workflow run. */
    @Test
    void anUnreachableChildFailsTheActionRatherThanThrowing() {
        IIActorSystem system = new IIActorSystem("parent");
        RecordingTransport transport = new RecordingTransport();
        transport.failure = new RuntimeException("Connection refused");
        try {
            RemoteActorIIAR remote = proxy("chat-ui:x", "x", transport, system);

            ActionResult result = remote.callByActionName("currentPromptText", "");

            assertFalse(result.isSuccess());
            assertTrue(result.getResult().contains("Connection refused"), result.getResult());
        } finally {
            system.terminate();
        }
    }

    // ---- resolution through the actor system ----

    @Test
    void anUnknownNameIsOfferedToTheRegisteredFactory() {
        IIActorSystem system = new IIActorSystem("parent");
        RecordingTransport transport = new RecordingTransport();
        try {
            system.addActorFactory(name -> name.startsWith("chat-ui:")
                    ? proxy(name, name.substring("chat-ui:".length()), transport, system)
                    : null);

            IIActorRef<?> found = system.getIIActor("chat-ui:project1/chat-01.chat");

            assertTrue(found instanceof RemoteActorIIAR);
            assertSame(found, system.getIIActor("chat-ui:project1/chat-01.chat"),
                    "a name resolved once must not be built again");
        } finally {
            system.terminate();
        }
    }

    @Test
    void aNameNoFactoryClaimsStaysUnresolved() {
        IIActorSystem system = new IIActorSystem("parent");
        try {
            system.addActorFactory(name -> null);

            assertNull(system.getIIActor("nobody:knows-this"));
        } finally {
            system.terminate();
        }
    }

    /** The built-in names keep their meaning: a factory must not be able to take "calc". */
    @Test
    void builtInNamesAreResolvedBeforeAnyFactory() {
        IIActorSystem system = new IIActorSystem("parent");
        RecordingTransport transport = new RecordingTransport();
        try {
            system.addActorFactory(name -> proxy(name, name, transport, system));

            IIActorRef<?> calc = system.getIIActor("calc");
            assertTrue(calc instanceof CalcActor, calc.getClass().getName());
        } finally {
            system.terminate();
        }
    }
}
