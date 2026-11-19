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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads column files of a primary-key table with data evolution. Rows are produced in the layout of
 * the value fields read from the table; fields the file does not hold are null.
 *
 * <p>No filter is pushed down: skipping a row group would break the row alignment with the base
 * file.
 */
public class ColumnFileReaderFactory {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final RowType readValueType;
    private final DataFilePathFactory pathFactory;
    private final CoreOptions options;
    private final FormatReaderMapping.Builder formatReaderMappingBuilder;
    private final ConcurrentHashMap<MappingKey, FormatReaderMapping> mappings;

    public ColumnFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType readValueType,
            FileFormatDiscover formatDiscover,
            DataFilePathFactory pathFactory,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.readValueType = readValueType;
        this.pathFactory = pathFactory;
        this.options = options;
        // column files are plain row files, their physical schema is the projected table schema
        this.formatReaderMappingBuilder =
                new FormatReaderMapping.Builder(
                        formatDiscover,
                        readValueType.getFields(),
                        TableSchema::fields,
                        null,
                        null,
                        null);
        this.mappings = new ConcurrentHashMap<>();
    }

    public RowType readValueType() {
        return readValueType;
    }

    /** Positions of the read value fields that the column file holds. */
    public boolean[] coveredFields(DataFileMeta file) {
        Set<Integer> fileFieldIds = new HashSet<>();
        for (DataField field : physicalSchema(file).fields()) {
            fileFieldIds.add(field.id());
        }
        List<DataField> readFields = readValueType.getFields();
        boolean[] covered = new boolean[readFields.size()];
        for (int i = 0; i < covered.length; i++) {
            covered[i] = fileFieldIds.contains(readFields.get(i).id());
        }
        return covered;
    }

    public RecordReader<InternalRow> createRecordReader(DataFileMeta file) throws IOException {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        FormatReaderMapping mapping =
                mappings.computeIfAbsent(
                        new MappingKey(file.schemaId(), file.writeCols(), formatIdentifier),
                        key ->
                                formatReaderMappingBuilder.build(
                                        formatIdentifier, schema, physicalSchema(file)));
        return new DataFileRecordReader(
                readValueType,
                mapping.getReaderFactory(),
                new FormatReaderContext(
                        fileIO, pathFactory.toPath(file), file.fileSize(), null, null),
                options.scanIgnoreCorruptFile(),
                options.scanIgnoreLostFile(),
                mapping.getIndexMapping(),
                mapping.getCastMapping(),
                null,
                false,
                null,
                -1,
                Collections.emptyMap());
    }

    private TableSchema physicalSchema(DataFileMeta file) {
        long schemaId = file.schemaId();
        TableSchema fileSchema = schemaId == schema.id() ? schema : schemaManager.schema(schemaId);
        return fileSchema.project(file.writeCols());
    }

    private static class MappingKey {

        private final long schemaId;
        private final List<String> writeCols;
        private final String formatIdentifier;

        private MappingKey(long schemaId, List<String> writeCols, String formatIdentifier) {
            this.schemaId = schemaId;
            this.writeCols = writeCols;
            this.formatIdentifier = formatIdentifier;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MappingKey)) {
                return false;
            }
            MappingKey that = (MappingKey) o;
            return schemaId == that.schemaId
                    && Objects.equals(writeCols, that.writeCols)
                    && Objects.equals(formatIdentifier, that.formatIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaId, writeCols, formatIdentifier);
        }
    }
}
