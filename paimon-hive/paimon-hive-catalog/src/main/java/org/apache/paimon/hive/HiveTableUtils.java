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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.FormatTable.Format;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;
import static org.apache.paimon.hive.HiveCatalog.INPUT_FORMAT_CLASS_NAME;
import static org.apache.paimon.hive.HiveCatalog.OUTPUT_FORMAT_CLASS_NAME;
import static org.apache.paimon.hive.HiveCatalog.PAIMON_TABLE_IDENTIFIER;
import static org.apache.paimon.hive.HiveCatalog.SERDE_CLASS_NAME;
import static org.apache.paimon.hive.HiveCatalog.isView;
import static org.apache.paimon.table.FormatTableOptions.FIELD_DELIMITER;

class HiveTableUtils {

    public static FormatTable convertToFormatTable(Table hiveTable) {
        if (isView(hiveTable)) {
            throw new UnsupportedOperationException("Hive view is not supported.");
        }

        Identifier identifier = new Identifier(hiveTable.getDbName(), hiveTable.getTableName());
        Map<String, String> options = new HashMap<>(hiveTable.getParameters());
        List<String> partitionKeys = getFieldNames(hiveTable.getPartitionKeys());
        RowType rowType = createRowType(hiveTable);
        String comment = options.remove(COMMENT_PROP);
        String location = hiveTable.getSd().getLocation();

        Format format;
        SerDeInfo serdeInfo = hiveTable.getSd().getSerdeInfo();
        String serLib = serdeInfo.getSerializationLib().toLowerCase();
        String inputFormat = hiveTable.getSd().getInputFormat();
        if (serLib.contains("parquet")) {
            format = Format.PARQUET;
        } else if (serLib.contains("orc")) {
            format = Format.ORC;
        } else if (inputFormat.contains("Text")) {
            format = Format.CSV;
            // hive default field delimiter is '\u0001'
            options.put(
                    FIELD_DELIMITER.key(),
                    serdeInfo.getParameters().getOrDefault(FIELD_DELIM, "\u0001"));
        } else {
            throw new UnsupportedOperationException("Unsupported table: " + hiveTable);
        }

        return FormatTable.builder()
                .identifier(identifier)
                .rowType(rowType)
                .partitionKeys(partitionKeys)
                .location(location)
                .format(format)
                .options(options)
                .comment(comment)
                .build();
    }

    /**
     * Applies the specified file format to the given Hive table, updating the table's storage
     * descriptor {@link StorageDescriptor} and serialization/deserialization info {@link SerDeInfo}
     * based on the provided options.
     */
    public static void applyFileFormatToTable(
            Table table, Options options, @Nullable FormatTable.Format fileFormat) {
        HiveFormat hiveFormat = HiveFormat.convertFormat(fileFormat);

        StorageDescriptor sd = table.getSd() != null ? table.getSd() : new StorageDescriptor();
        sd.setInputFormat(hiveFormat.getInputFormat());
        sd.setOutputFormat(hiveFormat.getOutputFormat());

        SerDeInfo serDeInfo = sd.getSerdeInfo() != null ? sd.getSerdeInfo() : new SerDeInfo();
        Map<String, String> serdeParameters = serDeInfo.getParameters();
        if (serdeParameters == null) {
            serDeInfo.setParameters(new HashMap<>());
        }
        serDeInfo.getParameters().putAll(hiveFormat.getSerdeParameters(options));
        serDeInfo.setSerializationLib(hiveFormat.getSerializationLib());

        sd.setSerdeInfo(serDeInfo);
        table.setSd(sd);
    }

    /** Get field names from field schemas. */
    private static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
        List<String> names = new ArrayList<>(fieldSchemas.size());
        for (FieldSchema fs : fieldSchemas) {
            names.add(fs.getName());
        }
        return names;
    }

    /** Create a Paimon's Schema from Hive table's columns and partition keys. */
    public static RowType createRowType(Table table) {
        List<FieldSchema> allCols = new ArrayList<>(table.getSd().getCols());
        allCols.addAll(table.getPartitionKeys());
        Pair<String[], DataType[]> columnInformation = extractColumnInformation(allCols);
        return RowType.builder()
                .fields(columnInformation.getRight(), columnInformation.getLeft())
                .build();
    }

    private static Pair<String[], DataType[]> extractColumnInformation(List<FieldSchema> allCols) {
        String[] colNames = new String[allCols.size()];
        DataType[] colTypes = new DataType[allCols.size()];

        for (int i = 0; i < allCols.size(); i++) {
            FieldSchema fs = allCols.get(i);
            colNames[i] = fs.getName();
            colTypes[i] =
                    HiveTypeUtils.toPaimonType(
                            TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
        }

        return Pair.of(colNames, colTypes);
    }

    /**
     * Enum representing various Hive file formats with their associated configuration details. Each
     * enum constant corresponds to a specific file format (e.g., ORC, Parquet, CSV), providing the
     * necessary classes for input format, output format, serialization library (SerDe), and
     * optional SerDe parameters.
     */
    enum HiveFormat {
        HIVE_PAIMON(
                PAIMON_TABLE_IDENTIFIER,
                INPUT_FORMAT_CLASS_NAME,
                OUTPUT_FORMAT_CLASS_NAME,
                SERDE_CLASS_NAME,
                options -> null),
        HIVE_ORC(
                Format.ORC.name(),
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                options -> Collections.singletonMap(FIELD_DELIM, options.get(FIELD_DELIMITER))),
        HIVE_PARQUET(
                Format.PARQUET.name(),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                options -> null),
        HIVE_CSV(
                Format.CSV.name(),
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                options -> null);

        private final String format;
        private final String inputFormat;
        private final String outputFormat;
        private final String serializationLib;
        private final Function<Options, Map<String, String>> serdeParameters;

        HiveFormat(
                String format,
                String inputFormat,
                String outputFormat,
                String serializationLib,
                Function<Options, Map<String, String>> serdeParameters) {
            this.format = format;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.serializationLib = serializationLib;
            this.serdeParameters = serdeParameters;
        }

        public String getFormat() {
            return format;
        }

        public String getInputFormat() {
            return inputFormat;
        }

        public String getOutputFormat() {
            return outputFormat;
        }

        public String getSerializationLib() {
            return serializationLib;
        }

        public Map<String, String> getSerdeParameters(Options options) {
            Map<String, String> parameters = serdeParameters.apply(options);
            return parameters != null ? parameters : Collections.emptyMap();
        }

        private static final Map<String, HiveFormat> FORMAT_MAP =
                Arrays.stream(values())
                        .collect(Collectors.toMap(HiveFormat::getFormat, Function.identity()));

        /** Convert a given format to its corresponding HiveFormat enum. */
        public static HiveFormat convertFormat(@Nullable FormatTable.Format format) {
            if (format == null) {
                return HIVE_PAIMON;
            }
            return FORMAT_MAP.getOrDefault(format.name(), HIVE_PAIMON);
        }
    }
}
