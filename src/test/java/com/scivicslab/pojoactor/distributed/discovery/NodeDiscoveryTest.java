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

package com.scivicslab.pojoactor.distributed.discovery;

import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.discovery.GridEngineNodeDiscovery;
import com.scivicslab.pojoactor.distributed.discovery.K8sNodeDiscovery;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscoveryFactory;
import com.scivicslab.pojoactor.distributed.discovery.SlurmNodeDiscovery;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NodeDiscovery implementations.
 *
 * <p>Note: These tests verify the interface contracts and basic logic.
 * Full integration tests require actual Slurm/Kubernetes/GridEngine environments.</p>
 */
@Tag("S_dist")
class NodeDiscoveryTest {

    @Test
    void testSlurmDiscoveryNotApplicable() {
        // In normal test environment, Slurm should not be detected
        NodeDiscovery discovery = new SlurmNodeDiscovery();

        // This test assumes we're not running in Slurm
        // If SLURM_JOB_NODELIST is set, this test will fail (which is acceptable)
        if (System.getenv("SLURM_JOB_NODELIST") == null) {
            assertFalse(discovery.isApplicable(),
                    "SlurmNodeDiscovery should not be applicable without SLURM environment variables");
        }
    }

    @Test
    void testK8sDiscoveryNotApplicable() {
        // In normal test environment, Kubernetes should not be detected
        NodeDiscovery discovery = new K8sNodeDiscovery();

        // This test assumes we're not running in Kubernetes
        if (System.getenv("POD_NAME") == null) {
            assertFalse(discovery.isApplicable(),
                    "K8sNodeDiscovery should not be applicable without Kubernetes environment variables");
        }
    }

    @Test
    void testGridEngineDiscoveryNotApplicable() {
        // In normal test environment, Grid Engine should not be detected
        NodeDiscovery discovery = new GridEngineNodeDiscovery();

        // This test assumes we're not running in Grid Engine
        if (System.getenv("PE_HOSTFILE") == null) {
            assertFalse(discovery.isApplicable(),
                    "GridEngineNodeDiscovery should not be applicable without Grid Engine environment variables");
        }
    }

    @Test
    void testDefaultPort() {
        NodeDiscovery slurmDiscovery = new SlurmNodeDiscovery();
        assertEquals(8080, slurmDiscovery.getMyPort(),
                "Default port should be 8080");

        NodeDiscovery k8sDiscovery = new K8sNodeDiscovery();
        assertEquals(8080, k8sDiscovery.getMyPort(),
                "Default port should be 8080");

        NodeDiscovery geDiscovery = new GridEngineNodeDiscovery();
        assertEquals(8080, geDiscovery.getMyPort(),
                "Default port should be 8080");
    }

    @Test
    void testCustomPort() {
        NodeDiscovery slurmDiscovery = new SlurmNodeDiscovery(9090);
        assertEquals(9090, slurmDiscovery.getMyPort(),
                "Custom port should be 9090");

        NodeDiscovery k8sDiscovery = new K8sNodeDiscovery(9090, "default");
        assertEquals(9090, k8sDiscovery.getMyPort(),
                "Custom port should be 9090");

        NodeDiscovery geDiscovery = new GridEngineNodeDiscovery(9090);
        assertEquals(9090, geDiscovery.getMyPort(),
                "Custom port should be 9090");
    }

    @Test
    void testAutoDetectInNormalEnvironment() {
        // In normal test environment, auto-detect should fail
        // (no Slurm, Kubernetes, or Grid Engine detected)

        boolean hasDistributedEnv =
                System.getenv("SLURM_JOB_NODELIST") != null ||
                System.getenv("POD_NAME") != null ||
                System.getenv("PE_HOSTFILE") != null;

        if (!hasDistributedEnv) {
            assertThrows(IllegalStateException.class, () -> {
                NodeDiscoveryFactory.autoDetect();
            }, "autoDetect() should throw exception when no environment is detected");
        }
    }

    @Test
    void testNodeInfoCreation() {
        NodeInfo node = new NodeInfo("node-1", "host1.example.com", 8080);

        assertEquals("node-1", node.getNodeId());
        assertEquals("host1.example.com", node.getHost());
        assertEquals(8080, node.getPort());
        assertEquals("http://host1.example.com:8080", node.getUrl());
    }

    /**
     * Simulated test for Slurm discovery logic.
     * This demonstrates how the discovery would work if environment variables were set.
     */
    @Test
    void testSlurmDiscoveryContract() {
        // This test verifies the contract, not actual functionality
        NodeDiscovery discovery = new SlurmNodeDiscovery(8081);

        assertEquals(8081, discovery.getMyPort());

        // If applicable, these methods should not throw
        if (discovery.isApplicable()) {
            assertDoesNotThrow(() -> discovery.getMyNodeId());
            assertDoesNotThrow(() -> discovery.getMyHost());
            assertDoesNotThrow(() -> {
                List<NodeInfo> nodes = discovery.getAllNodes();
                assertNotNull(nodes);
                assertFalse(nodes.isEmpty());
            });
        }
    }

    /**
     * Simulated test for Kubernetes discovery logic.
     */
    @Test
    void testK8sDiscoveryContract() {
        NodeDiscovery discovery = new K8sNodeDiscovery(8082, "test-namespace");

        assertEquals(8082, discovery.getMyPort());

        // If applicable, these methods should not throw
        if (discovery.isApplicable()) {
            assertDoesNotThrow(() -> discovery.getMyNodeId());
            assertDoesNotThrow(() -> discovery.getMyHost());
            assertDoesNotThrow(() -> {
                List<NodeInfo> nodes = discovery.getAllNodes();
                assertNotNull(nodes);
                assertFalse(nodes.isEmpty());
            });
        }
    }

    /**
     * Simulated test for Grid Engine discovery logic.
     */
    @Test
    void testGridEngineDiscoveryContract() {
        NodeDiscovery discovery = new GridEngineNodeDiscovery(8083);

        assertEquals(8083, discovery.getMyPort());

        // If applicable, these methods should not throw
        if (discovery.isApplicable()) {
            assertDoesNotThrow(() -> discovery.getMyNodeId());
            assertDoesNotThrow(() -> discovery.getMyHost());
            assertDoesNotThrow(() -> {
                List<NodeInfo> nodes = discovery.getAllNodes();
                assertNotNull(nodes);
                assertFalse(nodes.isEmpty());
            });
        }
    }
}
