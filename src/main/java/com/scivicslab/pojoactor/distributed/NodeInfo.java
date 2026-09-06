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

/**
 * Information about a node in the distributed actor system.
 *
 * <p>This class holds the network address and identification
 * information for a node that can host actors.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * NodeInfo node1 = new NodeInfo("node1", "192.168.1.10", 8081);
 * System.out.println(node1.getAddress()); // "192.168.1.10:8081"
 * System.out.println(node1.getUrl());     // "http://192.168.1.10:8081"
 * }</pre>
 *
 * @author devteam@scivicslab.com
 * @since 1.0.0
 */
public class NodeInfo {

    private final String nodeId;
    private final String host;
    private final int port;

    /**
     * Constructs a new NodeInfo with specified parameters.
     *
     * @param nodeId unique identifier for this node
     * @param host the hostname or IP address
     * @param port the HTTP port number
     */
    public NodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    /**
     * Returns the node identifier.
     *
     * @return the node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns the hostname or IP address.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the HTTP port number.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the network address in "host:port" format.
     *
     * @return the address string
     */
    public String getAddress() {
        return host + ":" + port;
    }

    /**
     * Returns the base URL for this node.
     *
     * @return the URL string (e.g., "http://192.168.1.10:8081")
     */
    public String getUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return nodeId.equals(nodeInfo.nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }
}
