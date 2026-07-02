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

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Request sent from writer subtasks to the Paimon writer coordinator. */
public class FileInfoRequest implements CoordinationRequest {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final int subtaskId;
    private final int attemptNumber;
    private final long watermark;
    private final byte[] serializedData;
    private final int committableCount;
    private final boolean recovered;
    private final @Nullable String commitUser;
    private final int payloadHash;

    public static FileInfoRequest fileInfo(
            long checkpointId,
            int subtaskId,
            int attemptNumber,
            long watermark,
            byte[] serializedData,
            int committableCount) {
        return new FileInfoRequest(
                checkpointId,
                subtaskId,
                attemptNumber,
                watermark,
                serializedData,
                committableCount,
                false,
                null);
    }

    public static FileInfoRequest recoveredFileInfo(
            long checkpointId,
            int subtaskId,
            int attemptNumber,
            long watermark,
            byte[] serializedData,
            int committableCount,
            String commitUser) {
        return new FileInfoRequest(
                checkpointId,
                subtaskId,
                attemptNumber,
                watermark,
                serializedData,
                committableCount,
                true,
                commitUser);
    }

    private FileInfoRequest(
            long checkpointId,
            int subtaskId,
            int attemptNumber,
            long watermark,
            byte[] serializedData,
            int committableCount,
            boolean recovered,
            @Nullable String commitUser) {
        this.checkpointId = checkpointId;
        this.subtaskId = subtaskId;
        this.attemptNumber = attemptNumber;
        this.watermark = watermark;
        this.serializedData = serializedData == null ? new byte[0] : serializedData;
        this.committableCount = committableCount;
        this.recovered = recovered;
        this.commitUser = commitUser;
        this.payloadHash = Arrays.hashCode(this.serializedData);
    }

    public long checkpointId() {
        return checkpointId;
    }

    public int subtaskId() {
        return subtaskId;
    }

    public int attemptNumber() {
        return attemptNumber;
    }

    public long watermark() {
        return watermark;
    }

    public byte[] serializedData() {
        return serializedData;
    }

    public int committableCount() {
        return committableCount;
    }

    public boolean recovered() {
        return recovered;
    }

    public @Nullable String commitUser() {
        return commitUser;
    }

    public boolean samePayload(FileInfoRequest other) {
        return other != null
                && payloadHash == other.payloadHash
                && committableCount == other.committableCount
                && Arrays.equals(serializedData, other.serializedData);
    }

    @Override
    public String toString() {
        if (recovered) {
            return String.format(
                    "FileInfoRequest{checkpoint=%d, recovered=true, subtask=%d, attempt=%d, "
                            + "count=%d, dataSize=%d bytes, commitUser=%s}",
                    checkpointId,
                    subtaskId,
                    attemptNumber,
                    committableCount,
                    serializedData.length,
                    commitUser);
        }
        return String.format(
                "FileInfoRequest{checkpoint=%d, subtask=%d, attempt=%d, count=%d, dataSize=%d bytes}",
                checkpointId, subtaskId, attemptNumber, committableCount, serializedData.length);
    }
}
