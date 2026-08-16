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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Reads one projected vector value from every physical row of a data file. */
public class PkVectorDataFileReader implements PkVectorReader {

    private final DataFileMeta dataFile;
    private final int dimension;
    private final RecordReaderIterator<KeyValue> iterator;
    private long rowsRead;

    public PkVectorDataFileReader(
            KeyValueFileReaderFactory readerFactory, DataFileMeta dataFile, int dimension)
            throws IOException {
        checkArgument(dimension > 0, "Vector dimension must be positive.");
        this.dataFile = dataFile;
        this.dimension = dimension;
        this.iterator = new RecordReaderIterator<>(readerFactory.createRecordReader(dataFile));
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public long rowCount() {
        return dataFile.rowCount();
    }

    @Override
    public boolean readNextVector(float[] reuse) {
        checkArgument(reuse.length == dimension, "Vector buffer dimension does not match reader.");
        checkArgument(
                rowsRead < rowCount(), "Read past data file %s row count.", dataFile.fileName());
        checkArgument(
                iterator.hasNext(),
                "Data file %s ended before its declared row count.",
                dataFile.fileName());
        KeyValue keyValue = iterator.next();
        rowsRead++;
        if (rowsRead == rowCount()) {
            checkArgument(
                    !iterator.hasNext(),
                    "Data file %s contains more rows than declared.",
                    dataFile.fileName());
        }
        InternalRow value = keyValue.value();
        if (value.isNullAt(0)) {
            return false;
        }
        InternalArray vector = value.getVector(0);
        checkArgument(
                vector.size() == dimension,
                "Vector in data file %s has dimension %s instead of %s.",
                dataFile.fileName(),
                vector.size(),
                dimension);
        for (int i = 0; i < dimension; i++) {
            reuse[i] = vector.getFloat(i);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            iterator.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close vector reader for " + dataFile.fileName(), e);
        }
    }

    /** Bucket-scoped factory with vector-only value projection and no deletion-vector filtering. */
    public static class Factory {

        private final KeyValueFileReaderFactory readerFactory;
        private final int dimension;

        public Factory(
                KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                BinaryRow partition,
                int bucket,
                DataField vectorField,
                int dimension) {
            this.readerFactory =
                    readerFactoryBuilder
                            .copyWithoutProjection()
                            .withReadKeyType(RowType.of())
                            .withReadValueType(RowType.of(vectorField))
                            .buildWithoutDeletionVector(partition, bucket);
            this.dimension = dimension;
        }

        public PkVectorDataFileReader create(DataFileMeta dataFile) throws IOException {
            return new PkVectorDataFileReader(readerFactory, dataFile, dimension);
        }
    }
}
