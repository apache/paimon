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

package org.apache.paimon.flink.pulsar;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * This cursor would leave pulsar start consuming from a specific message id.
 */
public class PartitionedStartCursor implements StartCursor {
    private static final long serialVersionUID = 1L;

    private final Map<Integer, CursorPosition> positionMap = new HashMap<>();

    public PartitionedStartCursor(Map<Integer, MessageId> messageIdMap) {
        for (Map.Entry<Integer, MessageId> entry : messageIdMap.entrySet()) {
            positionMap.put(entry.getKey(), new CursorPosition(entry.getValue(), true));
        }
    }

    @Override
    public CursorPosition position(String topic, int partitionId) {
        CursorPosition cursorPosition = positionMap.get(partitionId);
        if (cursorPosition == null) {
            cursorPosition = new CursorPosition(new MessageIdImpl(-1L, -1L, partitionId), true);
        }
        return cursorPosition;
    }
}
