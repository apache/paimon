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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.table.source.DataSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Used as a placeholder for an empty snapshot, indicating that the current snapshot does not
 * contain any {@link org.apache.paimon.table.source.Split}.
 */
public class PlaceholderSplit extends DataSplit {

    private static final long serialVersionUID = 3L;
    private static final String NO_USE_BUCKET_PATH = "/no-used";

    private final DataSplit dataSplit;

    public PlaceholderSplit(long snapshotId) {
        dataSplit =
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withBeforeFiles(Collections.emptyList())
                        .withBucket(0)
                        .withDataFiles(Collections.emptyList())
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .isStreaming(true)
                        .rawConvertible(false)
                        .withBucketPath(NO_USE_BUCKET_PATH)
                        .build();
    }

    public PlaceholderSplit(DataSplit dataSplit) {
        this.dataSplit = dataSplit;
    }

    @Override
    public long snapshotId() {
        return dataSplit.snapshotId();
    }

    @Override
    public BinaryRow partition() {
        return dataSplit.partition();
    }

    @Override
    public int bucket() {
        return dataSplit.bucket();
    }

    @Override
    public List<DataFileMeta> beforeFiles() {
        return dataSplit.beforeFiles();
    }

    @Override
    public List<DataFileMeta> dataFiles() {
        return dataSplit.dataFiles();
    }

    @Override
    public boolean isStreaming() {
        return dataSplit.isStreaming();
    }

    @Override
    public long rowCount() {
        return dataSplit.rowCount();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PlaceholderSplit that = (PlaceholderSplit) o;
        return Objects.equals(dataSplit, that.dataSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSplit);
    }

    public void serialize(DataOutputView out) throws IOException {
        dataSplit.serialize(out);
    }

    public static PlaceholderSplit deserialize(DataInputView in) throws IOException {
        DataSplit dataSplit = DataSplit.deserialize(in);
        return new PlaceholderSplit(dataSplit);
    }
}
