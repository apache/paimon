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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;

/**
 * Context containing all necessary information for building global indexes.
 *
 * <p>This class is serializable to support Spark distributed execution. The partition is stored
 * both as a transient {@link BinaryRow} and as serialized bytes to ensure proper serialization
 * across executor nodes.
 */
public class GlobalIndexBuilderContext implements Serializable {

    private final transient SparkSession spark;
    private final FileStoreTable table;
    private final BinaryRowSerializer binaryRowSerializer;
    private final transient BinaryRow partition;
    private final byte[] partitionBytes;
    private final RowType readType;
    private final DataField indexField;
    private final String indexType;
    private final Range rowRange;
    private final Options options;

    public GlobalIndexBuilderContext(
            SparkSession spark,
            FileStoreTable table,
            BinaryRow partition,
            RowType readType,
            DataField indexField,
            String indexType,
            Range rowRange,
            Options options) {
        this.spark = spark;
        this.table = table;
        this.partition = partition;
        this.readType = readType;
        this.indexField = indexField;
        this.indexType = indexType;
        this.rowRange = rowRange;
        this.options = options;

        this.binaryRowSerializer = new BinaryRowSerializer(partition.getFieldCount());
        try {
            this.partitionBytes = binaryRowSerializer.serializeToBytes(partition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SparkSession spark() {
        return spark;
    }

    public BinaryRow partitionFromBytes() {
        try {
            return binaryRowSerializer.deserializeFromBytes(partitionBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BinaryRow partition() {
        return partition;
    }

    public FileStoreTable table() {
        return table;
    }

    public RowType readType() {
        return readType;
    }

    public DataField indexField() {
        return indexField;
    }

    public String indexType() {
        return indexType;
    }

    public Range range() {
        return rowRange;
    }

    public Options options() {
        return options;
    }

    public GlobalIndexFileReadWrite globalIndexFileReadWrite() {
        FileIO fileIO = table.fileIO();
        IndexPathFactory indexPathFactory =
                table.store().pathFactory().indexFileFactory(partitionFromBytes(), 0);
        return new GlobalIndexFileReadWrite(fileIO, indexPathFactory);
    }
}
