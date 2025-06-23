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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.write.RestoreFiles;
import org.apache.paimon.operation.write.WriteRestore;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * {@link WriteRestore} to restore files from write coordinator, there is cache in coordinator, this
 * is to avoid a large number of duplicate scans of manifest files.
 */
public class CoordinatedWriteRestore implements WriteRestore {

    private final TaskOperatorEventGateway gateway;
    private final OperatorID operatorID;

    public CoordinatedWriteRestore(TaskOperatorEventGateway gateway, OperatorID operatorID) {
        this.gateway = gateway;
        this.operatorID = operatorID;
    }

    @Override
    public long latestCommittedIdentifier(String user) {
        LatestIdentifierRequest request = new LatestIdentifierRequest(user);
        try {
            SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
            LatestIdentifierResponse response =
                    (LatestIdentifierResponse)
                            gateway.sendRequestToCoordinator(operatorID, serializedRequest).get();
            return response.latestIdentifier();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(
                        serializeBinaryRow(partition),
                        bucket,
                        scanDynamicBucketIndex,
                        scanDeleteVectorsIndex);
        try {
            SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
            ScanCoordinationResponse response =
                    (ScanCoordinationResponse)
                            gateway.sendRequestToCoordinator(operatorID, serializedRequest).get();
            return new RestoreFiles(
                    response.snapshot(),
                    response.totalBuckets(),
                    response.extractDataFiles(),
                    response.extractDynamicBucketIndex(),
                    response.extractDeletionVectorsIndex());
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
