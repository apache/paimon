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

package org.apache.paimon.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader.RecordIterator;

/**
 * A {@link DataEvolutionIterator} that is also a {@link FileRecordIterator}. The file path and
 * returned position are delegated to one designated inner iterator, so a row assembled from
 * multiple files reports one deterministic member file of its row-id group and its position within
 * that group (all inner iterators are row-aligned).
 */
public class DataEvolutionFileRecordIterator extends DataEvolutionIterator
        implements FileRecordIterator<InternalRow> {

    private final FileRecordIterator<InternalRow> designated;

    public DataEvolutionFileRecordIterator(
            DataEvolutionRow row,
            RecordIterator<InternalRow>[] iterators,
            FileRecordIterator<InternalRow> designated) {
        super(row, iterators);
        this.designated = designated;
    }

    @Override
    public long returnedPosition() {
        return designated.returnedPosition();
    }

    @Override
    public Path filePath() {
        return designated.filePath();
    }
}
