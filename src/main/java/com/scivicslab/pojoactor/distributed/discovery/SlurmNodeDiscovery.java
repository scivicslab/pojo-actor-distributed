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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Node discovery implementation for Slurm workload manager.
 *
 * <p>This implementation reads Slurm environment variables to discover
 * nodes in the cluster:</p>
 * <ul>
 * <li>{@code SLURM_JOB_NODELIST} - List of nodes allocated to the job</li>
 * <li>{@code SLURM_PROCID} - Process rank (0-based index)</li>
 * <li>{@code SLURM_NTASKS} - Total number of tasks</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * #!/bin/bash
 * #SBATCH --nodes=10
 * #SBATCH --ntasks-per-node=1
 *
 * srun java -jar myapp.jar --port=8080
 * }</pre>
 *
 * <pre>{@code
 * public static void main(String[] args) {
 *     NodeDiscovery discovery = new SlurmNodeDiscovery();
 *
 *     if (!discovery.isApplicable()) {
 *         System.err.println("Not running in Slurm environment");
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
public class SlurmNodeDiscovery implements NodeDiscovery {

    private static final Logger logger = Logger.getLogger(SlurmNodeDiscovery.class.getName());

    private final int defaultPort;
    private List<String> cachedHostnames = null;

    /**
     * Constructs a SlurmNodeDiscovery with default port 8080.
     */
    public SlurmNodeDiscovery() {
        this(8080);
    }

    /**
     * Constructs a SlurmNodeDiscovery with specified port.
     *
     * @param defaultPort the default port for actor communication
     */
    public SlurmNodeDiscovery(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public boolean isApplicable() {
        return System.getenv("SLURM_JOB_NODELIST") != null
                && System.getenv("SLURM_PROCID") != null;
    }

    @Override
    public String getMyNodeId() {
        String procId = System.getenv("SLURM_PROCID");
        if (procId == null) {
            throw new IllegalStateException("SLURM_PROCID environment variable not set");
        }
        return "node-" + procId;
    }

    @Override
    public String getMyHost() {
        // Get hostname from current node
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
     * Retrieves the list of hostnames from Slurm.
     *
     * <p>This method executes {@code scontrol show hostnames} to parse
     * the SLURM_JOB_NODELIST environment variable.</p>
     *
     * @return list of hostnames
     */
    private List<String> getHostnames() {
        if (cachedHostnames != null) {
            return cachedHostnames;
        }

        String nodeList = System.getenv("SLURM_JOB_NODELIST");
        if (nodeList == null) {
            throw new IllegalStateException("SLURM_JOB_NODELIST environment variable not set");
        }

        List<String> hostnames = new ArrayList<>();

        try {
            // Use scontrol to parse node list
            Process process = Runtime.getRuntime().exec(
                    new String[]{"scontrol", "show", "hostnames", nodeList}
            );

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    hostnames.add(line.trim());
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("scontrol command failed with exit code " + exitCode);
            }

            cachedHostnames = hostnames;
            logger.info(String.format("Discovered %d Slurm nodes", hostnames.size()));

        } catch (IOException | InterruptedException e) {
            logger.log(Level.SEVERE, "Failed to parse SLURM_JOB_NODELIST", e);
            throw new RuntimeException("Failed to discover Slurm nodes", e);
        }

        return hostnames;
    }
}
