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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Node discovery implementation for Grid Engine (SGE/UGE/OGE).
 *
 * <p>This implementation reads the Grid Engine PE_HOSTFILE to discover
 * nodes in the parallel environment. It requires:</p>
 * <ul>
 * <li>{@code PE_HOSTFILE} - Path to the hostfile listing allocated nodes</li>
 * <li>{@code SGE_TASK_ID} - Task ID (for array jobs, optional)</li>
 * </ul>
 *
 * <p>The PE_HOSTFILE format is:</p>
 * <pre>
 * node01.cluster.local 4 all.q@node01.cluster.local UNDEFINED
 * node02.cluster.local 4 all.q@node02.cluster.local UNDEFINED
 * </pre>
 *
 * <h2>Grid Engine Script Example</h2>
 * <pre>{@code
 * #!/bin/bash
 * #$ -pe mpi 10
 * #$ -cwd
 *
 * # Launch one process per node
 * mpirun -np $NSLOTS java -jar myapp.jar --port=8080
 * }</pre>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * public static void main(String[] args) {
 *     NodeDiscovery discovery = new GridEngineNodeDiscovery();
 *
 *     if (!discovery.isApplicable()) {
 *         System.err.println("Not running in Grid Engine environment");
 *         System.exit(1);
 *     }
 *
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
 * }
 * }</pre>
 *
 * @author devteam@scivicslab.com
 * @since 1.0.0
 */
public class GridEngineNodeDiscovery implements NodeDiscovery {

    private static final Logger logger = Logger.getLogger(GridEngineNodeDiscovery.class.getName());

    private final int defaultPort;
    private List<String> cachedHostnames = null;

    /**
     * Constructs a GridEngineNodeDiscovery with default port 8080.
     */
    public GridEngineNodeDiscovery() {
        this(8080);
    }

    /**
     * Constructs a GridEngineNodeDiscovery with specified port.
     *
     * @param defaultPort the default port for actor communication
     */
    public GridEngineNodeDiscovery(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public boolean isApplicable() {
        return System.getenv("PE_HOSTFILE") != null;
    }

    @Override
    public String getMyNodeId() {
        // Try to use SGE_TASK_ID if available (for array jobs)
        String taskId = System.getenv("SGE_TASK_ID");
        if (taskId != null && !taskId.equals("undefined")) {
            return "node-task-" + taskId;
        }

        // Otherwise, find our position in the hostfile
        String myHostname = getMyHost();
        List<String> hostnames = getHostnames();

        for (int i = 0; i < hostnames.size(); i++) {
            if (hostnames.get(i).equals(myHostname)) {
                return "node-" + i;
            }
        }

        // Fallback: use hostname
        return "node-" + myHostname;
    }

    @Override
    public String getMyHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.log(Level.WARNING, "Failed to get local hostname, using 0.0.0.0", e);
            return "0.0.0.0";
        }
    }

    @Override
    public int getMyPort() {
        return defaultPort;
    }

    @Override
    public List<NodeInfo> getAllNodes() {
        List<String> hostnames = getHostnames();
        List<NodeInfo> nodes = new ArrayList<>();

        for (int i = 0; i < hostnames.size(); i++) {
            String nodeId = "node-" + i;
            String hostname = hostnames.get(i);
            nodes.add(new NodeInfo(nodeId, hostname, defaultPort));
        }

        return nodes;
    }

    /**
     * Reads and parses the PE_HOSTFILE to extract hostnames.
     *
     * <p>The PE_HOSTFILE format is:</p>
     * <pre>
     * hostname slots queue processor
     * </pre>
     *
     * @return list of unique hostnames
     */
    private List<String> getHostnames() {
        if (cachedHostnames != null) {
            return cachedHostnames;
        }

        String hostfilePath = System.getenv("PE_HOSTFILE");
        if (hostfilePath == null) {
            throw new IllegalStateException("PE_HOSTFILE environment variable not set");
        }

        List<String> hostnames = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(hostfilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // Parse line: hostname slots queue processor
                String[] parts = line.split("\\s+");
                if (parts.length >= 1) {
                    String hostname = parts[0];

                    // Add hostname only once (PE_HOSTFILE may list same host multiple times for slots)
                    if (!hostnames.contains(hostname)) {
                        hostnames.add(hostname);
                    }
                }
            }

            cachedHostnames = hostnames;
            logger.info(String.format("Discovered %d Grid Engine nodes", hostnames.size()));

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to read PE_HOSTFILE", e);
            throw new RuntimeException("Failed to discover Grid Engine nodes", e);
        }

        return hostnames;
    }
}
