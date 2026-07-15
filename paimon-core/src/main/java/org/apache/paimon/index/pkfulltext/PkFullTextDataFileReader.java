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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Reads one projected text value from every physical row of a data file. */
public class PkFullTextDataFileReader implements Closeable {

    private final DataFileMeta dataFile;
    private final RecordReaderIterator<KeyValue> iterator;
    private long rowsRead;

    public PkFullTextDataFileReader(KeyValueFileReaderFactory readerFactory, DataFileMeta dataFile)
            throws IOException {
        this.dataFile = dataFile;
        this.iterator = new RecordReaderIterator<>(readerFactory.createRecordReader(dataFile));
    }

    public long rowCount() {
        return dataFile.rowCount();
    }

    @Nullable
    public BinaryString readNextText() {
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
        return value.isNullAt(0) ? null : value.getString(0);
    }

    @Override
    public void close() throws IOException {
        try {
            iterator.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close full-text reader for " + dataFile.fileName(), e);
        }
    }

    /** Bucket-scoped factory with text-only value projection and no deletion-vector filtering. */
    public static class Factory {

        private final KeyValueFileReaderFactory readerFactory;

        public Factory(
                KeyValueFileReaderFactory.Builder readerFactoryBuilder,
                BinaryRow partition,
                int bucket,
                DataField textField) {
            this.readerFactory =
                    readerFactoryBuilder
                            .copyWithoutProjection()
                            .withReadKeyType(RowType.of())
                            .withReadValueType(RowType.of(textField))
                            .buildWithoutDeletionVector(partition, bucket);
        }

        public PkFullTextDataFileReader create(DataFileMeta dataFile) throws IOException {
            return new PkFullTextDataFileReader(readerFactory, dataFile);
        }
    }
}
