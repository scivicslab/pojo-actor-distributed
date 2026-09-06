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
import java.util.List;

/**
 * Interface for node discovery in distributed actor systems.
 *
 * <p>Implementations of this interface provide environment-specific logic
 * to discover nodes in distributed computing environments such as Slurm,
 * Kubernetes, or Grid Engine.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Auto-detect environment and discover nodes
 * NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();
 *
 * // Get current node information
 * String myNodeId = discovery.getMyNodeId();
 * String myHost = discovery.getMyHost();
 * int myPort = 8080;
 *
 * // Create distributed actor system
 * DistributedActorSystem system =
 *     new DistributedActorSystem(myNodeId, myHost, myPort);
 *
 * // Register all remote nodes
 * for (NodeInfo node : discovery.getAllNodes()) {
 *     if (!node.getNodeId().equals(myNodeId)) {
 *         system.registerRemoteNode(
 *             node.getNodeId(),
 *             node.getHost(),
 *             node.getPort()
 *         );
 *     }
 * }
 * }</pre>
 *
 * @author devteam@scivicslab.com
 * @since 2.5.0
 */
public interface NodeDiscovery {

    /**
     * Returns the unique identifier for the current node.
     *
     * @return the node ID
     */
    String getMyNodeId();

    /**
     * Returns the hostname or IP address for the current node.
     *
     * @return the host address
     */
    String getMyHost();

    /**
     * Returns the default port number for actor communication.
     *
     * <p>This can be overridden by the user if needed.</p>
     *
     * @return the port number
     */
    default int getMyPort() {
        return 8080;
    }

    /**
     * Returns information about all nodes in the cluster.
     *
     * <p>This includes the current node and all remote nodes.</p>
     *
     * @return list of all node information
     */
    List<NodeInfo> getAllNodes();

    /**
     * Checks if this discovery implementation is applicable to
     * the current environment.
     *
     * @return true if this discovery can be used in current environment
     */
    boolean isApplicable();
}
