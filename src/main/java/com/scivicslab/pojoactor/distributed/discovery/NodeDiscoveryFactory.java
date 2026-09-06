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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Factory for automatic node discovery based on runtime environment.
 *
 * <p>This factory detects the execution environment (Slurm, Kubernetes,
 * Grid Engine) and returns an appropriate {@link NodeDiscovery} implementation.</p>
 *
 * <h2>Supported Environments</h2>
 * <ul>
 * <li><strong>Slurm</strong> - Detects SLURM_JOB_NODELIST environment variable</li>
 * <li><strong>Kubernetes</strong> - Detects POD_NAME and KUBERNETES_SERVICE_HOST</li>
 * <li><strong>Grid Engine</strong> - Detects PE_HOSTFILE environment variable</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * public static void main(String[] args) throws IOException {
 *     // Auto-detect environment and discover nodes
 *     NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();
 *
 *     // Create distributed actor system
 *     DistributedActorSystem system = new DistributedActorSystem(
 *         discovery.getMyNodeId(),
 *         discovery.getMyHost(),
 *         discovery.getMyPort()
 *     );
 *
 *     // Register all remote nodes
 *     for (NodeInfo node : discovery.getAllNodes()) {
 *         if (!node.getNodeId().equals(discovery.getMyNodeId())) {
 *             system.registerRemoteNode(
 *                 node.getNodeId(),
 *                 node.getHost(),
 *                 node.getPort()
 *             );
 *         }
 *     }
 *
 *     // Register actors and start processing
 *     // ...
 * }
 * }</pre>
 *
 * @author devteam@scivicslab.com
 * @since 1.0.0
 */
public class NodeDiscoveryFactory {

    private static final Logger logger = Logger.getLogger(NodeDiscoveryFactory.class.getName());

    /**
     * Automatically detects the execution environment and returns
     * an appropriate NodeDiscovery implementation.
     *
     * <p>Detection priority:</p>
     * <ol>
     * <li>Slurm (checks SLURM_JOB_NODELIST)</li>
     * <li>Kubernetes (checks POD_NAME and KUBERNETES_SERVICE_HOST)</li>
     * <li>Grid Engine (checks PE_HOSTFILE)</li>
     * </ol>
     *
     * @return a NodeDiscovery implementation for the detected environment
     * @throws IllegalStateException if no supported environment is detected
     */
    public static NodeDiscovery autoDetect() {
        return autoDetect(8080);
    }

    /**
     * Automatically detects the execution environment and returns
     * an appropriate NodeDiscovery implementation with specified port.
     *
     * @param port the default port for actor communication
     * @return a NodeDiscovery implementation for the detected environment
     * @throws IllegalStateException if no supported environment is detected
     */
    public static NodeDiscovery autoDetect(int port) {
        List<NodeDiscovery> candidates = new ArrayList<>();
        candidates.add(new SlurmNodeDiscovery(port));
        candidates.add(new K8sNodeDiscovery(port, "default"));
        candidates.add(new GridEngineNodeDiscovery(port));

        for (NodeDiscovery discovery : candidates) {
            if (discovery.isApplicable()) {
                logger.info(String.format("Detected environment: %s",
                        discovery.getClass().getSimpleName()));
                return discovery;
            }
        }

        throw new IllegalStateException(
                "No supported distributed environment detected. " +
                "Ensure you are running in Slurm, Kubernetes, or Grid Engine.");
    }

    // Private constructor to prevent instantiation
    private NodeDiscoveryFactory() {
    }
}
