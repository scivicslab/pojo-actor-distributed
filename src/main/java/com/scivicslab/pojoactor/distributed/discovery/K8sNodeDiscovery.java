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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Node discovery implementation for Kubernetes StatefulSets.
 *
 * <p>This implementation uses Kubernetes Headless Service DNS to discover
 * pods in a StatefulSet. It requires the following environment variables:</p>
 * <ul>
 * <li>{@code POD_NAME} - Name of the current pod (e.g., "pojo-actor-0")</li>
 * <li>{@code SERVICE_NAME} - Name of the Headless Service</li>
 * <li>{@code REPLICAS} - Number of replicas in the StatefulSet</li>
 * </ul>
 *
 * <h2>Kubernetes Configuration Example</h2>
 * <pre>{@code
 * apiVersion: v1
 * kind: Service
 * metadata:
 *   name: actor-nodes
 * spec:
 *   clusterIP: None  # Headless Service
 *   selector:
 *     app: pojo-actor
 *   ports:
 *   - port: 8080
 * ---
 * apiVersion: apps/v1
 * kind: StatefulSet
 * metadata:
 *   name: pojo-actor
 * spec:
 *   serviceName: actor-nodes
 *   replicas: 10
 *   template:
 *     spec:
 *       containers:
 *       - name: actor-node
 *         image: myapp:v2
 *         env:
 *         - name: POD_NAME
 *           valueFrom:
 *             fieldRef:
 *               fieldPath: metadata.name
 *         - name: SERVICE_NAME
 *           value: "actor-nodes"
 *         - name: REPLICAS
 *           value: "10"
 *         ports:
 *         - containerPort: 8080
 * }</pre>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * public static void main(String[] args) {
 *     NodeDiscovery discovery = new K8sNodeDiscovery();
 *
 *     if (!discovery.isApplicable()) {
 *         System.err.println("Not running in Kubernetes environment");
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
 * @since 2.5.0
 */
public class K8sNodeDiscovery implements NodeDiscovery {

    private static final Logger logger = Logger.getLogger(K8sNodeDiscovery.class.getName());

    private final int defaultPort;
    private final String namespace;

    /**
     * Constructs a K8sNodeDiscovery with default port 8080 and default namespace.
     */
    public K8sNodeDiscovery() {
        this(8080, "default");
    }

    /**
     * Constructs a K8sNodeDiscovery with specified port and namespace.
     *
     * @param defaultPort the default port for actor communication
     * @param namespace the Kubernetes namespace
     */
    public K8sNodeDiscovery(int defaultPort, String namespace) {
        this.defaultPort = defaultPort;
        this.namespace = namespace;
    }

    @Override
    public boolean isApplicable() {
        return System.getenv("POD_NAME") != null
                && System.getenv("SERVICE_NAME") != null
                && System.getenv("KUBERNETES_SERVICE_HOST") != null;
    }

    @Override
    public String getMyNodeId() {
        String podName = System.getenv("POD_NAME");
        if (podName == null) {
            throw new IllegalStateException("POD_NAME environment variable not set");
        }
        return podName;
    }

    @Override
    public String getMyHost() {
        String podName = System.getenv("POD_NAME");
        String serviceName = System.getenv("SERVICE_NAME");

        if (podName == null || serviceName == null) {
            throw new IllegalStateException(
                    "POD_NAME and SERVICE_NAME environment variables must be set");
        }

        // StatefulSet pod DNS: <pod-name>.<service-name>.<namespace>.svc.cluster.local
        return String.format("%s.%s.%s.svc.cluster.local", podName, serviceName, namespace);
    }

    @Override
    public int getMyPort() {
        return defaultPort;
    }

    @Override
    public List<NodeInfo> getAllNodes() {
        String serviceName = System.getenv("SERVICE_NAME");
        String replicasStr = System.getenv("REPLICAS");

        if (serviceName == null || replicasStr == null) {
            throw new IllegalStateException(
                    "SERVICE_NAME and REPLICAS environment variables must be set");
        }

        int replicas;
        try {
            replicas = Integer.parseInt(replicasStr);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("REPLICAS must be a valid integer", e);
        }

        List<NodeInfo> nodes = new ArrayList<>();

        // StatefulSet pods are named: <statefulset-name>-0, <statefulset-name>-1, ...
        // Extract StatefulSet name from POD_NAME (remove trailing -<number>)
        String podName = System.getenv("POD_NAME");
        String statefulSetName = podName.replaceAll("-\\d+$", "");

        for (int i = 0; i < replicas; i++) {
            String nodeId = statefulSetName + "-" + i;
            String hostname = String.format(
                    "%s.%s.%s.svc.cluster.local",
                    nodeId, serviceName, namespace
            );

            // Verify hostname is resolvable (optional, but good for debugging)
            try {
                InetAddress.getByName(hostname);
                nodes.add(new NodeInfo(nodeId, hostname, defaultPort));
            } catch (UnknownHostException e) {
                logger.log(Level.WARNING,
                        String.format("Failed to resolve hostname: %s", hostname), e);
                // Still add the node, it might become available later
                nodes.add(new NodeInfo(nodeId, hostname, defaultPort));
            }
        }

        logger.info(String.format("Discovered %d Kubernetes pods", nodes.size()));
        return nodes;
    }
}
