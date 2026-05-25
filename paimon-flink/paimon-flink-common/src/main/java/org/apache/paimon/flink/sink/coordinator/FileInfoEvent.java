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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/** File info event to coordinator. */
public class FileInfoEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    // Format：| Count (4B) | Len1 (4B) | Data1 | Len2 (4B) | Data2 | ... |
    private final byte[] serializedData;
    private final long checkpoint;

    public FileInfoEvent(byte[] serializedData, long checkpoint) {
        this.serializedData = serializedData;
        this.checkpoint = checkpoint;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }

    public long checkpoint() {
        return checkpoint;
    }

    @Override
    public String toString() {
        return String.format(
                "FileInfoEvent{checkpoint=%d, dataSize=%d bytes}",
                checkpoint, serializedData.length);
    }
}
