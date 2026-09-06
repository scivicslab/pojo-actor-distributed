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

import com.scivicslab.pojoactor.core.Action;
import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.turingworkflow.workflow.IIActorRef;
import com.scivicslab.pojoactor.distributed.DistributedIIActorSystem;
import com.scivicslab.pojoactor.distributed.RemoteActorRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@Tag("Y_dist")
@Tag("J_dist")
class DistributedActorTest {

    private DistributedIIActorSystem system1;
    private DistributedIIActorSystem system2;

    @BeforeEach
    void setUp() throws IOException {
        // Create two actor systems on different ports
        system1 = new DistributedIIActorSystem("node1", "localhost", 9081);
        system2 = new DistributedIIActorSystem("node2", "localhost", 9082);
    }

    @AfterEach
    void tearDown() {
        if (system1 != null) {
            system1.terminate();
        }
        if (system2 != null) {
            system2.terminate();
        }
    }

    @Test
    void testRemoteActorInvocation() throws Exception {
        // Register math actor on node1
        MathPlugin math = new MathPlugin();
        TestMathIIAR mathActor = new TestMathIIAR("math", math, system1);
        system1.addIIActor(mathActor);

        // Register node1 as remote node in node2
        system2.registerRemoteNode("node1", "localhost", 9081);

        // Get remote actor reference
        RemoteActorRef remoteMath = system2.getRemoteActor("node1", "math");

        // Invoke remote actor
        ActionResult result = remoteMath.callByActionName("add", "5,3");

        assertTrue(result.isSuccess());
        assertEquals("8", result.getResult());
    }

    @Test
    void testRemoteActorNotFound() throws Exception {
        // Register node1 in node2 without any actors
        system2.registerRemoteNode("node1", "localhost", 9081);

        // Try to call non-existent actor
        RemoteActorRef remoteActor = system2.getRemoteActor("node1", "nonexistent");
        ActionResult result = remoteActor.callByActionName("someAction", "args");

        assertFalse(result.isSuccess());
        assertTrue(result.getResult().contains("not found"));
    }

    @Test
    void testMultipleRemoteActors() throws Exception {
        // Register two actors on node1
        MathPlugin math = new MathPlugin();
        TestMathIIAR mathActor = new TestMathIIAR("math", math, system1);
        system1.addIIActor(mathActor);

        MathPlugin calculator = new MathPlugin();
        TestMathIIAR calcActor = new TestMathIIAR("calculator", calculator, system1);
        system1.addIIActor(calcActor);

        // Register node1 in node2
        system2.registerRemoteNode("node1", "localhost", 9081);

        // Call both actors from node2
        RemoteActorRef remoteMath = system2.getRemoteActor("node1", "math");
        RemoteActorRef remoteCalc = system2.getRemoteActor("node1", "calculator");

        ActionResult result1 = remoteMath.callByActionName("add", "10,20");
        ActionResult result2 = remoteCalc.callByActionName("multiply", "3,4");

        assertTrue(result1.isSuccess());
        assertEquals("30", result1.getResult());

        assertTrue(result2.isSuccess());
        assertEquals("12", result2.getResult());
    }

    @Test
    void testBidirectionalCommunication() throws Exception {
        // Register actors on both nodes
        MathPlugin math1 = new MathPlugin();
        TestMathIIAR mathActor1 = new TestMathIIAR("math", math1, system1);
        system1.addIIActor(mathActor1);

        MathPlugin math2 = new MathPlugin();
        TestMathIIAR mathActor2 = new TestMathIIAR("math", math2, system2);
        system2.addIIActor(mathActor2);

        // Register each node as remote in the other
        system1.registerRemoteNode("node2", "localhost", 9082);
        system2.registerRemoteNode("node1", "localhost", 9081);

        // Call from node1 to node2
        RemoteActorRef remote2 = system1.getRemoteActor("node2", "math");
        ActionResult result1 = remote2.callByActionName("add", "100,200");

        // Call from node2 to node1
        RemoteActorRef remote1 = system2.getRemoteActor("node1", "math");
        ActionResult result2 = remote1.callByActionName("multiply", "5,6");

        assertTrue(result1.isSuccess());
        assertEquals("300", result1.getResult());

        assertTrue(result2.isSuccess());
        assertEquals("30", result2.getResult());
    }

    @Test
    void testHealthCheck() throws Exception {
        // Health check should work even without actors
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://localhost:9081/health"))
                .GET()
                .build();

        java.net.http.HttpResponse<String> response = client.send(
                request,
                java.net.http.HttpResponse.BodyHandlers.ofString()
        );

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("healthy"));
        assertTrue(response.body().contains("node1"));
    }

    /**
     * Test implementation of IIActorRef for MathPlugin.
     */
    private static class TestMathIIAR extends IIActorRef<MathPlugin> {

        public TestMathIIAR(String actorName, MathPlugin object, DistributedIIActorSystem system) {
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
