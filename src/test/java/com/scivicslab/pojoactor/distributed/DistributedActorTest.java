/*
 * Copyright 2025 devteam@scivicslab.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.Action;
import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.discovery.StaticNodeDiscovery;
import com.scivicslab.turingworkflow.workflow.IIActorRef;
import com.scivicslab.turingworkflow.workflow.IIActorSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two merged systems talking over real HTTP on loopback. Everything one system calls on the
 * other goes through the other's allow-list and the target actor's mailbox.
 */
@Tag("Y_dist")
@Tag("J_dist")
class DistributedActorTest {

    private DistributedIIActorSystem system1;
    private DistributedIIActorSystem system2;
    private int port1;
    private int port2;

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static Set<AllowListDispatcher.AllowedAction> mathActions(String actor) {
        return Set.of(
                new AllowListDispatcher.AllowedAction(actor, "add"),
                new AllowListDispatcher.AllowedAction(actor, "multiply"));
    }

    @BeforeEach
    void setUp() throws IOException {
        port1 = freePort();
        port2 = freePort();
        NodeInfo n1 = new NodeInfo("node1", "127.0.0.1", port1);
        NodeInfo n2 = new NodeInfo("node2", "127.0.0.1", port2);
        system1 = DistributedIIActorSystem.builder()
                .discovery(new StaticNodeDiscovery(n1, List.of(n2)))
                .allowedActions(mathActions("math"))
                .build();
        system2 = DistributedIIActorSystem.builder()
                .discovery(new StaticNodeDiscovery(n2, List.of(n1)))
                .allowedActions(mathActions("math"))
                .build();
        system1.startHttpServer("127.0.0.1", port1);
        system2.startHttpServer("127.0.0.1", port2);
    }

    @AfterEach
    void tearDown() {
        if (system1 != null) system1.terminate();
        if (system2 != null) system2.terminate();
    }

    @Test
    void testRemoteActorInvocation() {
        system1.addIIActor(new TestMathIIAR("math", new MathPlugin(), system1));

        ActionResult result = system2.getRemoteActor("node1", "math").callByActionName("add", "5,3");

        assertTrue(result.isSuccess(), result.getResult());
        assertEquals("8", result.getResult());
    }

    /** The old path called any action of any registered actor. Now only the listed pairs. */
    @Test
    void anActionNotOnTheAllowListIsRefused() {
        system1.addIIActor(new TestMathIIAR("math", new MathPlugin(), system1));

        ActionResult result = system2.getRemoteActor("node1", "math").callByActionName("greet", "x");

        assertFalse(result.isSuccess());
        assertTrue(result.getResult().contains("Not allowed"), result.getResult());
    }

    @Test
    void anActorNotOnTheAllowListIsRefusedLikeAnUnknownOne() {
        system1.addIIActor(new TestMathIIAR("secret", new MathPlugin(), system1));

        ActionResult existing = system2.getRemoteActor("node1", "secret").callByActionName("add", "1,2");
        ActionResult missing = system2.getRemoteActor("node1", "nonexistent").callByActionName("add", "1,2");

        assertFalse(existing.isSuccess());
        assertFalse(missing.isSuccess());
        assertTrue(existing.getResult().startsWith("Not allowed"));
        assertTrue(missing.getResult().startsWith("Not allowed"));
    }

    @Test
    void testMultipleRemoteActors() {
        system1.terminate();
        system1 = null;
        // A second system with two listed actors.
        try {
            int p = freePort();
            NodeInfo n1 = new NodeInfo("node1", "127.0.0.1", p);
            system1 = DistributedIIActorSystem.builder()
                    .discovery(new StaticNodeDiscovery(n1, List.of()))
                    .allowedActions(Set.of(
                            new AllowListDispatcher.AllowedAction("math", "add"),
                            new AllowListDispatcher.AllowedAction("calculator", "multiply")))
                    .build();
            system1.startHttpServer("127.0.0.1", p);
            system1.addIIActor(new TestMathIIAR("math", new MathPlugin(), system1));
            system1.addIIActor(new TestMathIIAR("calculator", new MathPlugin(), system1));
            system2.registerRemoteNode("node1", "127.0.0.1", p);

            ActionResult r1 = system2.getRemoteActor("node1", "math").callByActionName("add", "10,20");
            ActionResult r2 = system2.getRemoteActor("node1", "calculator").callByActionName("multiply", "3,4");

            assertEquals("30", r1.getResult());
            assertEquals("12", r2.getResult());
        } catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void testBidirectionalCommunication() {
        system1.addIIActor(new TestMathIIAR("math", new MathPlugin(), system1));
        system2.addIIActor(new TestMathIIAR("math", new MathPlugin(), system2));

        ActionResult result1 = system1.getRemoteActor("node2", "math").callByActionName("add", "100,200");
        ActionResult result2 = system2.getRemoteActor("node1", "math").callByActionName("multiply", "5,6");

        assertEquals("300", result1.getResult());
        assertEquals("30", result2.getResult());
    }

    @Test
    void sendGivesAReceiptWhenTheRemoteMailboxHasTheCall() {
        system1.addIIActor(new TestMathIIAR("math", new MathPlugin(), system1));

        Receipt receipt = system2.getRemoteActor("node1", "math").send("add", "1,2");

        assertTrue(receipt.accepted(), receipt.detail());
    }

    @Test
    void thereIsNoServerOfItsOwnBeforeOneIsStarted() throws IOException {
        int p = freePort();
        DistributedIIActorSystem idle = DistributedIIActorSystem.builder()
                .discovery(new StaticNodeDiscovery(new NodeInfo("idle", "127.0.0.1", p), List.of()))
                .build();
        try {
            assertThrows(IOException.class, () -> {
                try (java.net.Socket s = new java.net.Socket("127.0.0.1", p)) { }
            });
        } finally {
            idle.terminate();
        }
    }

    /** Test implementation of IIActorRef for MathPlugin. */
    private static class TestMathIIAR extends IIActorRef<MathPlugin> {

        public TestMathIIAR(String actorName, MathPlugin object, IIActorSystem system) {
            super(actorName, object, system);
        }

        @Action("add")
        public ActionResult add(String args) { return this.object.callByActionName("add", args); }

        @Action("multiply")
        public ActionResult multiply(String args) { return this.object.callByActionName("multiply", args); }

        @Action("getLastResult")
        public ActionResult getLastResult(String args) { return this.object.callByActionName("getLastResult", args); }

        @Action("greet")
        public ActionResult greet(String args) { return this.object.callByActionName("greet", args); }
    }
}
