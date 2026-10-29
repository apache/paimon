/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink;

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Reads uids and operator ids off a built {@link StreamGraph}, for the operator-uid coverage tests.
 *
 * <p>The tests uid everything they build themselves with {@link #TEST_UID_PREFIX}, so an operator
 * without a uid, or with a uid lacking the prefix, can only have come from Paimon.
 */
public final class OperatorUidGraphs {

    public static final String TEST_UID_PREFIX = "test-";

    private OperatorUidGraphs() {}

    /** Names of the operators without a uid. */
    public static List<String> operatorsWithoutUid(StreamGraph graph) {
        return graph.getStreamNodes().stream()
                .filter(node -> node.getTransformationUID() == null)
                .map(StreamNode::getOperatorName)
                .sorted()
                .collect(Collectors.toList());
    }

    /** Operator name to uid for every node, the uid being null where none was assigned. */
    public static Map<String, String> uidsByOperatorName(StreamGraph graph) {
        Map<String, String> uids = new TreeMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            uids.put(node.getOperatorName(), node.getTransformationUID());
        }
        return uids;
    }

    /** Operator name to uid for the nodes Paimon built and gave a uid. */
    public static Map<String, String> paimonUidsByOperatorName(StreamGraph graph) {
        Map<String, String> uids = new TreeMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String uid = node.getTransformationUID();
            if (uid != null && !uid.startsWith(TEST_UID_PREFIX)) {
                uids.put(node.getOperatorName(), uid);
            }
        }
        return uids;
    }

    /**
     * Uid to operator id for the nodes Paimon built. A checkpoint entry is keyed by the operator
     * id, so this map is what has to stay put as the rest of the job changes.
     */
    public static Map<String, String> paimonOperatorIdsByUid(StreamGraph graph) {
        Map<Integer, byte[]> hashes =
                new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
        Map<String, String> ids = new TreeMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            String uid = node.getTransformationUID();
            if (uid != null && !uid.startsWith(TEST_UID_PREFIX)) {
                ids.put(uid, hex(hashes.get(node.getId())));
            }
        }
        return ids;
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte b : bytes) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }
}
