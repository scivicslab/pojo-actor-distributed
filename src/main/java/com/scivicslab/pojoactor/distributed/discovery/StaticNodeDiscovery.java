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

/**
 * A {@link NodeDiscovery} that returns the nodes the caller configured.
 *
 * <p>For machines that are not a Slurm, Kubernetes or Grid Engine job, such as several
 * instances on one workstation. It probes nothing, so it is always applicable;
 * {@link NodeDiscoveryFactory#autoDetect()} never returns it. The caller chooses it.</p>
 *
 * <pre>{@code
 * NodeInfo me = new NodeInfo("a", "127.0.0.1", 28001);
 * NodeDiscovery d = new StaticNodeDiscovery(me, StaticNodeDiscovery.parsePeers("b=127.0.0.1:28002"));
 * }</pre>
 *
 * @since 1.1.0
 */
public class StaticNodeDiscovery implements NodeDiscovery {

    private final NodeInfo me;
    private final List<NodeInfo> all;

    /**
     * @param me    this node
     * @param peers the other nodes; an entry with this node's id is ignored
     */
    public StaticNodeDiscovery(NodeInfo me, List<NodeInfo> peers) {
        this.me = me;
        List<NodeInfo> nodes = new ArrayList<>();
        nodes.add(me);
        for (NodeInfo peer : peers) {
            if (!peer.getNodeId().equals(me.getNodeId())) {
                nodes.add(peer);
            }
        }
        this.all = List.copyOf(nodes);
    }

    /**
     * Parses a peer list written as {@code nodeId=host:port}, separated by commas.
     *
     * @param spec e.g. {@code "b=127.0.0.1:28002,c=10.0.0.3:28003"}; {@code null} or blank is no peers
     * @return the peers, in the order written
     * @throws IllegalArgumentException if an entry is not of that form; the message names the entry
     */
    public static List<NodeInfo> parsePeers(String spec) {
        List<NodeInfo> peers = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return peers;
        }
        for (String raw : spec.split(",")) {
            String entry = raw.trim();
            int eq = entry.indexOf('=');
            int colon = entry.lastIndexOf(':');
            if (eq <= 0 || colon <= eq + 1 || colon == entry.length() - 1) {
                throw new IllegalArgumentException("Peer must be nodeId=host:port: " + entry);
            }
            try {
                peers.add(new NodeInfo(entry.substring(0, eq),
                        entry.substring(eq + 1, colon),
                        Integer.parseInt(entry.substring(colon + 1))));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Peer must be nodeId=host:port: " + entry, e);
            }
        }
        return peers;
    }

    @Override
    public String getMyNodeId() {
        return me.getNodeId();
    }

    @Override
    public String getMyHost() {
        return me.getHost();
    }

    @Override
    public int getMyPort() {
        return me.getPort();
    }

    @Override
    public List<NodeInfo> getAllNodes() {
        return all;
    }

    @Override
    public boolean isApplicable() {
        return true;
    }
}
