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

package org.apache.paimon.operation;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RowDataFileRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BulkFormatMapping;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreRead} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreRead implements FileStoreRead<InternalRow> {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final long schemaId;
    private final RowType rowType;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, BulkFormatMapping> bulkFormatMappings;

    private int[][] projection;

    @Nullable private List<Predicate> filters;

    public AppendOnlyFileStoreRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.bulkFormatMappings = new HashMap<>();

        this.projection = Projection.range(0, rowType.getFieldCount()).toNestedIndexes();
    }

    public FileStoreRead<InternalRow> withProjection(int[][] projectedFields) {
        projection = projectedFields;
        return this;
    }

    @Override
    public FileStoreRead<InternalRow> withFilter(Predicate predicate) {
        this.filters = splitAnd(predicate);
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(split.partition(), split.bucket());
        List<ConcatRecordReader.ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();
        if (split.beforeFiles().size() > 0) {
            throw new UnsupportedOperationException(
                    "Append only not support before files now, "
                            + "please create an issue to describe your requirements.");
        }
        for (DataFileMeta file : split.dataFiles()) {
            String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
            BulkFormatMapping bulkFormatMapping =
                    bulkFormatMappings.computeIfAbsent(
                            new FormatKey(file.schemaId(), formatIdentifier),
                            key -> {
                                TableSchema tableSchema = schemaManager.schema(this.schemaId);
                                TableSchema dataSchema = schemaManager.schema(key.schemaId);
                                int[][] dataProjection =
                                        SchemaEvolutionUtil.createDataProjection(
                                                tableSchema.fields(),
                                                dataSchema.fields(),
                                                projection);
                                RowType rowType = dataSchema.logicalRowType();
                                IndexCastMapping indexCastMapping =
                                        SchemaEvolutionUtil.createIndexCastMapping(
                                                Projection.of(projection).toTopLevelIndexes(),
                                                tableSchema.fields(),
                                                Projection.of(dataProjection).toTopLevelIndexes(),
                                                dataSchema.fields());
                                List<Predicate> dataFilters =
                                        this.schemaId == key.schemaId
                                                ? filters
                                                : SchemaEvolutionUtil.createDataFilters(
                                                        tableSchema.fields(),
                                                        dataSchema.fields(),
                                                        filters);
                                return new BulkFormatMapping(
                                        indexCastMapping.getIndexMapping(),
                                        indexCastMapping.getCastMapping(),
                                        formatDiscover
                                                .discover(formatIdentifier)
                                                .createReaderFactory(
                                                        rowType, dataProjection, dataFilters));
                            });
            suppliers.add(
                    () ->
                            new RowDataFileRecordReader(
                                    fileIO,
                                    dataFilePathFactory.toPath(file.fileName()),
                                    bulkFormatMapping.getReaderFactory(),
                                    bulkFormatMapping.getIndexMapping(),
                                    bulkFormatMapping.getCastMapping()));
        }

        return ConcatRecordReader.create(suppliers);
    }
}
