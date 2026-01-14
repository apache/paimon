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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.stats.SimpleStatsEvolutions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A wrapper class for {@link DataSplit} that adds query authorization information. This class
 * delegates all Split interface methods to the wrapped DataSplit, while providing additional auth
 * result functionality.
 */
public class QueryAuthSplit extends DataSplit {

    private static final long serialVersionUID = 1L;

    private DataSplit dataSplit;
    @Nullable private TableQueryAuthResult authResult;

    public QueryAuthSplit(DataSplit dataSplit, @Nullable TableQueryAuthResult authResult) {
        this.dataSplit = dataSplit;
        this.authResult = authResult;
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    @Nullable
    public TableQueryAuthResult authResult() {
        return authResult;
    }

    // Delegate all DataSplit methods to the wrapped instance

    public long snapshotId() {
        return dataSplit.snapshotId();
    }

    public BinaryRow partition() {
        return dataSplit.partition();
    }

    public int bucket() {
        return dataSplit.bucket();
    }

    public String bucketPath() {
        return dataSplit.bucketPath();
    }

    @Nullable
    public Integer totalBuckets() {
        return dataSplit.totalBuckets();
    }

    public List<DataFileMeta> beforeFiles() {
        return dataSplit.beforeFiles();
    }

    public Optional<List<DeletionFile>> beforeDeletionFiles() {
        return dataSplit.beforeDeletionFiles();
    }

    public List<DataFileMeta> dataFiles() {
        return dataSplit.dataFiles();
    }

    @Override
    public Optional<List<DeletionFile>> deletionFiles() {
        return dataSplit.deletionFiles();
    }

    public boolean isStreaming() {
        return dataSplit.isStreaming();
    }

    public boolean rawConvertible() {
        return dataSplit.rawConvertible();
    }

    public OptionalLong latestFileCreationEpochMillis() {
        return dataSplit.latestFileCreationEpochMillis();
    }

    public OptionalLong earliestFileCreationEpochMillis() {
        return dataSplit.earliestFileCreationEpochMillis();
    }

    public long rowCount() {
        return dataSplit.rowCount();
    }

    public boolean mergedRowCountAvailable() {
        return dataSplit.mergedRowCountAvailable();
    }

    public long mergedRowCount() {
        return dataSplit.mergedRowCount();
    }

    public Object minValue(
            int fieldIndex,
            org.apache.paimon.types.DataField dataField,
            SimpleStatsEvolutions evolutions) {
        return dataSplit.minValue(fieldIndex, dataField, evolutions);
    }

    public Object maxValue(
            int fieldIndex,
            org.apache.paimon.types.DataField dataField,
            SimpleStatsEvolutions evolutions) {
        return dataSplit.maxValue(fieldIndex, dataField, evolutions);
    }

    public Long nullCount(int fieldIndex, SimpleStatsEvolutions evolutions) {
        return dataSplit.nullCount(fieldIndex, evolutions);
    }

    public long partialMergedRowCount() {
        return dataSplit.partialMergedRowCount();
    }

    @Override
    public Optional<List<RawFile>> convertToRawFiles() {
        return dataSplit.convertToRawFiles();
    }

    @Override
    @Nullable
    public Optional<List<IndexFile>> indexFiles() {
        return dataSplit.indexFiles();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryAuthSplit that = (QueryAuthSplit) o;
        return Objects.equals(dataSplit, that.dataSplit)
                && Objects.equals(authResult, that.authResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSplit, authResult);
    }

    @Override
    public String toString() {
        return "QueryAuthSplit{" + "dataSplit=" + dataSplit + ", authResult=" + authResult + '}';
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new org.apache.paimon.io.DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        QueryAuthSplit other = deserialize(new DataInputViewStreamWrapper(in));
        this.dataSplit = other.dataSplit;
        this.authResult = other.authResult;
    }

    public void serialize(DataOutputView out) throws IOException {
        // Serialize the wrapped DataSplit
        dataSplit.serialize(out);

        // Serialize authResult
        if (authResult != null) {
            out.writeBoolean(true);
            TableQueryAuthResultSerializer.serialize(authResult, out);
        } else {
            out.writeBoolean(false);
        }
    }

    public static QueryAuthSplit deserialize(DataInputView in) throws IOException {
        // Deserialize the wrapped DataSplit
        DataSplit dataSplit = DataSplit.deserialize(in);

        // Deserialize authResult
        TableQueryAuthResult authResult = null;
        if (in.readBoolean()) {
            authResult = TableQueryAuthResultSerializer.deserialize(in);
        }

        return new QueryAuthSplit(dataSplit, authResult);
    }

    public static QueryAuthSplit wrap(
            DataSplit dataSplit, @Nullable TableQueryAuthResult authResult) {
        if (authResult == null || authResult.isEmpty()) {
            return new QueryAuthSplit(dataSplit, null);
        }
        return new QueryAuthSplit(dataSplit, authResult);
    }
}
