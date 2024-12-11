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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.AbstractDataTableRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import java.io.IOException;

import static org.apache.paimon.table.source.KeyValueTableRead.unwrap;

/** An {@link InnerTableRead} that reads the data changed before and after compaction. */
public class LookupCompactDiffRead extends AbstractDataTableRead<KeyValue> {
    private final SplitRead<InternalRow> fullPhaseMergeRead;
    private final SplitRead<InternalRow> incrementalDiffRead;

    public LookupCompactDiffRead(MergeFileSplitRead mergeRead, TableSchema schema) {
        super(schema);
        this.incrementalDiffRead = new IncrementalCompactDiffSplitRead(mergeRead);
        this.fullPhaseMergeRead =
                SplitRead.convert(mergeRead, split -> unwrap(mergeRead.createReader(split)));
    }

    @Override
    public void applyReadType(RowType readType) {
        fullPhaseMergeRead.withReadType(readType);
        incrementalDiffRead.withReadType(readType);
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        DataSplit dataSplit = (DataSplit) split;
        if (dataSplit.beforeFiles().isEmpty()) {
            return fullPhaseMergeRead.createReader(dataSplit); // full reading phase
        } else {
            return incrementalDiffRead.createReader((DataSplit) split);
        }
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        fullPhaseMergeRead.withFilter(predicate);
        incrementalDiffRead.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableRead withIndexFilter(Predicate indexPredicate) {
        if (indexPredicate == null) {
            return this;
        }
        throw new UnsupportedOperationException("index should not be pushed down in the lookup");
    }

    @Override
    public InnerTableRead forceKeepDelete() {
        fullPhaseMergeRead.forceKeepDelete();
        incrementalDiffRead.forceKeepDelete();
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        fullPhaseMergeRead.withIOManager(ioManager);
        incrementalDiffRead.withIOManager(ioManager);
        return this;
    }
}
