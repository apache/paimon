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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Timestamp;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Parquet file has self-describing schema which may differ from the user required schema (e.g.
 * schema evolution). This factory is used to retrieve user required typed data via corresponding
 * reader which reads the underlying data. Part of the code is referred from Apache Hive.
 */
public final class ParquetDataColumnReaderFactory {

    private ParquetDataColumnReaderFactory() {}

    /**
     * The default data column reader for existing Parquet page reader which works for both
     * dictionary or non dictionary types, Mirror from dictionary encoding path.
     */
    private static class DefaultParquetDataColumnReader implements ParquetDataColumnReader {

        private final ValuesReader valuesReader;
        private final Dictionary dict;
        private final boolean isUtcTimestamp;

        public DefaultParquetDataColumnReader(ValuesReader valuesReader, boolean isUtcTimestamp) {
            this.valuesReader = checkNotNull(valuesReader);
            this.dict = null;
            this.isUtcTimestamp = isUtcTimestamp;
        }

        public DefaultParquetDataColumnReader(Dictionary dict, boolean isUtcTimestamp) {
            this.valuesReader = null;
            this.dict = checkNotNull(dict);
            this.isUtcTimestamp = isUtcTimestamp;
        }

        @Override
        public void initFromPage(int i, ByteBufferInputStream in) throws IOException {
            valuesReader.initFromPage(i, in);
        }

        @Override
        public boolean readBoolean() {
            return valuesReader.readBoolean();
        }

        @Override
        public boolean readBoolean(int id) {
            return dict.decodeToBoolean(id);
        }

        @Override
        public byte[] readBytes() {
            return valuesReader.readBytes().getBytesUnsafe();
        }

        @Override
        public byte[] readBytes(int id) {
            return dict.decodeToBinary(id).getBytesUnsafe();
        }

        @Override
        public float readFloat() {
            return valuesReader.readFloat();
        }

        @Override
        public float readFloat(int id) {
            return dict.decodeToFloat(id);
        }

        @Override
        public double readDouble() {
            return valuesReader.readDouble();
        }

        @Override
        public double readDouble(int id) {
            return dict.decodeToDouble(id);
        }

        @Override
        public Timestamp readMillsTimestamp() {
            return Timestamp.fromEpochMillis(valuesReader.readLong());
        }

        @Override
        public Timestamp readMicrosTimestamp() {
            return Timestamp.fromMicros(valuesReader.readLong());
        }

        @Override
        public Timestamp readNanosTimestamp() {
            return int96TimestampConvert(valuesReader.readBytes());
        }

        @Override
        public Timestamp readMillsTimestamp(int id) {
            return Timestamp.fromEpochMillis(readLong(id));
        }

        @Override
        public Timestamp readMicrosTimestamp(int id) {
            return Timestamp.fromMicros(readLong(id));
        }

        @Override
        public Timestamp readNanosTimestamp(int id) {
            return int96TimestampConvert(dict.decodeToBinary(id));
        }

        @Override
        public int readInteger() {
            return valuesReader.readInteger();
        }

        @Override
        public int readInteger(int id) {
            return dict.decodeToInt(id);
        }

        @Override
        public long readLong(int id) {
            return dict.decodeToLong(id);
        }

        @Override
        public long readLong() {
            return valuesReader.readLong();
        }

        @Override
        public int readSmallInt() {
            return valuesReader.readInteger();
        }

        @Override
        public int readSmallInt(int id) {
            return dict.decodeToInt(id);
        }

        @Override
        public int readTinyInt() {
            return valuesReader.readInteger();
        }

        @Override
        public int readTinyInt(int id) {
            return dict.decodeToInt(id);
        }

        @Override
        public int readValueDictionaryId() {
            return valuesReader.readValueDictionaryId();
        }

        @Override
        public Dictionary getDictionary() {
            return dict;
        }

        private Timestamp int96TimestampConvert(Binary binary) {
            ByteBuffer buf = binary.toByteBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long timeOfDayNanos = buf.getLong();
            int julianDay = buf.getInt();
            return TimestampColumnReader.int96ToTimestamp(
                    isUtcTimestamp, timeOfDayNanos, julianDay);
        }
    }

    private static ParquetDataColumnReader getDataColumnReaderByTypeHelper(
            boolean isDictionary,
            @Nullable Dictionary dictionary,
            @Nullable ValuesReader valuesReader,
            boolean isUtcTimestamp) {
        return isDictionary
                ? new DefaultParquetDataColumnReader(dictionary, isUtcTimestamp)
                : new DefaultParquetDataColumnReader(valuesReader, isUtcTimestamp);
    }

    public static ParquetDataColumnReader getDataColumnReaderByTypeOnDictionary(
            Dictionary realReader, boolean isUtcTimestamp) {
        return getDataColumnReaderByTypeHelper(true, realReader, null, isUtcTimestamp);
    }

    public static ParquetDataColumnReader getDataColumnReaderByType(
            ValuesReader realReader, boolean isUtcTimestamp) {
        return getDataColumnReaderByTypeHelper(false, null, realReader, isUtcTimestamp);
    }
}
