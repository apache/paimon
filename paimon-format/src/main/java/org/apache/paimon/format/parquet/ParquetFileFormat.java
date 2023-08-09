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

package org.apache.paimon.format.parquet;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.format.parquet.ParquetFileFormatFactory.IDENTIFIER;

/** Parquet {@link FileFormat}. */
public class ParquetFileFormat extends FileFormat {

    private final FormatContext formatContext;

    public ParquetFileFormat(FormatContext formatContext) {
        super(IDENTIFIER);
        this.formatContext = formatContext;
    }

    @VisibleForTesting
    Options formatOptions() {
        return formatContext.formatOptions();
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType type, int[][] projection, List<Predicate> filters) {
        return new ParquetReaderFactory(
                getParquetConfiguration(formatContext.formatOptions()),
                Projection.of(projection).project(type),
                formatContext.readBatchSize());
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        List<String> disableDictionaryFields =
                getDicDisabledFieldPath(type, formatContext.getDictionaryOptions());
        return new ParquetWriterFactory(
                new RowDataParquetBuilder(
                        type,
                        getParquetConfiguration(formatContext.formatOptions()),
                        disableDictionaryFields));
    }

    @Override
    public List<String> getAllFieldPath(RowType type) {
        List<String> result = new ArrayList<>();

        List<DataField> fields = type.getFields();
        for (DataField field : fields) {
            DataType dataType = field.type();
            if (dataType.getTypeRoot() == DataTypeRoot.ROW) {
                result.addAll(traversal(Pair.of(field.name(), (RowType) dataType)));
            } else {
                result.add(field.name());
            }
        }

        return result;
    }

    public static List<String> traversal(Pair<String, RowType> pathPrefixAndRow) {
        List<String> result = new ArrayList<>();

        LinkedList<Pair<String, RowType>> queue = new LinkedList<>();
        queue.offer(pathPrefixAndRow);
        while (!queue.isEmpty()) {
            Pair<String, RowType> polled = queue.poll();

            String pathPrefix = polled.getKey();
            List<DataField> datafields = polled.getValue().getFields();
            for (DataField dataField : datafields) {
                DataType dataType = dataField.type();
                String name = dataField.name();
                String path = String.format("%s.%s", pathPrefix, name);
                if (dataType.getTypeRoot() != DataTypeRoot.ROW) {
                    result.add(path);
                } else {
                    queue.offer(Pair.of(path, (RowType) dataType));
                }
            }
        }

        return result;
    }

    @Override
    public void validateDataFields(RowType rowType) {
        ParquetSchemaConverter.convertToParquetMessageType("paimon_schema", rowType);
    }

    @Override
    public Optional<TableStatsExtractor> createStatsExtractor(
            RowType type, FieldStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new ParquetTableStatsExtractor(type, statsCollectors));
    }

    public static Options getParquetConfiguration(Options options) {
        Options conf = new Options();
        options.toMap().forEach((key, value) -> conf.setString(IDENTIFIER + "." + key, value));
        return conf;
    }
}
