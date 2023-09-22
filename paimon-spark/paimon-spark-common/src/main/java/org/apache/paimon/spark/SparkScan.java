/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.options.Options;
import org.apache.paimon.spark.cdc.CDCCol;
import org.apache.paimon.spark.sources.PaimonMicroBatchStream;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.OptionalLong;

/**
 * A Spark {@link Scan} for paimon.
 *
 * <p>TODO Introduce a SparkRFScan to implement SupportsRuntimeFiltering.
 */
public class SparkScan implements Scan, SupportsReportStatistics {

    private final Table table;
    private final ReadBuilder readBuilder;

    private List<Split> splits;
    private final Options options;

    public SparkScan(Table table, ReadBuilder readBuilder) {
        this.table = table;
        this.readBuilder = readBuilder;
        this.options = Options.fromMap(table.options());
    }

    @Override
    public String description() {
        // TODO add filters
        return String.format("paimon(%s)", readBuilder.tableName());
    }

    @Override
    public StructType readSchema() {
        StructType schema = SparkTypeUtils.fromPaimonRowType(readBuilder.readType());
        return options.get(SparkConnectorOptions.READ_CHANGELOG)
                ? CDCCol.addCDCCols(schema)
                : schema;
    }

    @Override
    public Batch toBatch() {
        return new Batch() {
            @Override
            public InputPartition[] planInputPartitions() {
                return splits().stream()
                        .map(SparkInputPartition::new)
                        .toArray(InputPartition[]::new);
            }

            @Override
            public PartitionReaderFactory createReaderFactory() {
                return new SparkReaderFactory(readBuilder, options);
            }
        };
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new PaimonMicroBatchStream((FileStoreTable) table, readBuilder, checkpointLocation);
    }

    protected List<Split> splits() {
        if (splits == null) {
            splits = readBuilder.newScan().plan().splits();
        }
        return splits;
    }

    @Override
    public Statistics estimateStatistics() {
        long rowCount = 0L;

        for (Split split : splits()) {
            rowCount += split.rowCount();
        }

        final long numRows = rowCount;
        final long sizeInBytes = readSchema().defaultSize() * numRows;

        return new Statistics() {
            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }

            @Override
            public OptionalLong numRows() {
                return OptionalLong.of(numRows);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SparkScan that = (SparkScan) o;
        return readBuilder.equals(that.readBuilder);
    }

    @Override
    public int hashCode() {
        return readBuilder.hashCode();
    }
}
