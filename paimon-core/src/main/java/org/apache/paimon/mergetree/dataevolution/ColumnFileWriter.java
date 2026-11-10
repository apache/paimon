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

package org.apache.paimon.mergetree.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.LongCounter;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StatsCollectorFactories.createStatsFactories;

/**
 * Writes a column file of a primary-key table with data evolution.
 *
 * <p><b>Row order is a contract the caller must keep, and nothing can check it.</b> The caller
 * provides the new values of the columns for every row of the base file, in the order of the base
 * file. The rows of the base file that are not updated must carry their current values. Only the
 * row count is verified, here and again at commit: a column file whose rows are in another order is
 * committed and then stitched to the wrong rows, which silently corrupts the whole column. Callers
 * must derive the values by reading the base file, keeping its row order.
 *
 * <p>This Java API is the only entry point of the first version. There is no SQL entry point, and
 * no way to locate the row index of a primary key, so a caller cannot build a column file from a
 * {@code WHERE} clause. Both are planned by PIP-44; locating row indexes has to come first, because
 * a SQL entry point without it would expose the row order contract above to end users.
 */
public class ColumnFileWriter {

    private ColumnFileWriter() {}

    /**
     * Writes the column file and returns the commit message replacing {@code replacedFiles}, the
     * current column files of the base file holding these columns.
     */
    public static CommitMessage write(
            FileStoreTable table,
            BinaryRow partition,
            int bucket,
            DataFileMeta baseFile,
            List<String> columns,
            Iterator<InternalRow> rows,
            List<DataFileMeta> replacedFiles)
            throws Exception {
        TableSchema schema = table.schema();
        CoreOptions options = table.coreOptions();
        checkArgument(
                baseFile.firstRowId() != null && baseFile.writeCols() == null,
                "File %s is not a base file with an alignment range.",
                baseFile.fileName());
        checkArgument(!columns.isEmpty(), "Column file must hold at least one column.");
        for (String column : columns) {
            checkArgument(
                    !schema.primaryKeys().contains(column)
                            && !schema.partitionKeys().contains(column),
                    "Column file cannot hold primary key or partition column %s.",
                    column);
        }

        RowType writeType = schema.logicalRowType().project(columns);
        RowDataRollingFileWriter writer =
                new RowDataRollingFileWriter(
                        table.fileIO(),
                        schema.id(),
                        FileFormat.fileFormat(options),
                        Long.MAX_VALUE,
                        writeType,
                        table.store().pathFactory().createDataFilePathFactory(partition, bucket),
                        () -> new LongCounter(0),
                        options.fileCompression(),
                        createStatsFactories(options.statsMode(), options, columns),
                        options.indexColumnsOptions(),
                        FileSource.COMPACT,
                        options.asyncFileWrite(),
                        options.statsDenseStore(),
                        columns,
                        null,
                        Long.MAX_VALUE);
        Exception collected = null;
        try {
            while (rows.hasNext()) {
                writer.write(rows.next());
            }
        } catch (Exception e) {
            collected = e;
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                collected = ExceptionUtils.firstOrSuppressed(e, collected);
            }
        }
        if (collected != null) {
            writer.abort();
            throw collected;
        }

        List<DataFileMeta> result = writer.result();
        if (result.size() != 1 || result.get(0).rowCount() != baseFile.rowCount()) {
            writer.abort();
            throw new IllegalArgumentException(
                    String.format(
                            "Column file must have %s rows like base file %s, but has %s.",
                            baseFile.rowCount(),
                            baseFile.fileName(),
                            result.stream().mapToLong(DataFileMeta::rowCount).sum()));
        }
        DataFileMeta columnFile = result.get(0).assignFirstRowId(baseFile.firstRowId());
        return new CommitMessageImpl(
                partition,
                bucket,
                options.bucket(),
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        replacedFiles,
                        Collections.singletonList(columnFile),
                        Collections.emptyList()));
    }
}
