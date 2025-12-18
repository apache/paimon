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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.List;

public class DataEvolutionCompactTask {

    private final BinaryRow partition;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final boolean blobTask;

    public DataEvolutionCompactTask(
            BinaryRow partition, List<DataFileMeta> files, boolean blobTask) {
        this.partition = partition;
        this.compactBefore = new ArrayList<>(files);
        this.compactAfter = new ArrayList<>();
        this.blobTask = blobTask;
    }

    public BinaryRow partition() {
        return partition;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public boolean isBlobTask() {
        return blobTask;
    }
}
