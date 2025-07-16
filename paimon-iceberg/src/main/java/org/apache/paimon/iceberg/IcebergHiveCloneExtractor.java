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

package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.clone.HiveCloneExtractor;
import org.apache.paimon.hive.clone.HivePartitionFiles;
import org.apache.paimon.hive.clone.HiveTableCloneExtractor;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.CURRENT_SCHEMA;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DEFAULT_PARTITION_SPEC;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0;
import static org.apache.paimon.CoreOptions.FILE_COMPRESSION;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;

/** A {@link HiveCloneExtractor} for Iceberg tables. */
public class IcebergHiveCloneExtractor extends HiveTableCloneExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergHiveCloneExtractor.class);

    @Override
    public boolean matches(Table table) {
        return table.getParameters()
                .getOrDefault(TABLE_TYPE_PROP, "")
                .equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE);
    }

    @Override
    public List<HivePartitionFiles> extractFiles(
            Map<String, String> catalogOptions,
            IMetaStoreClient client,
            Table table,
            FileIO fileIO,
            Identifier identifier,
            RowType partitionRowType,
            String defaultPartitionName,
            @Nullable PartitionPredicate predicate)
            throws Exception {
        String metadataPath = table.getParameters().get(METADATA_LOCATION_PROP);
        String metadataJson = fileIO.readFileUtf8(new Path(metadataPath));
        TableMetadata metadata = TableMetadataParser.fromJson(metadataJson);
        Snapshot currentSnapshot = metadata.currentSnapshot();

        if (metadata.schemas().size() > 1) {
            LOG.warn("more than 1 schemas in iceberg!");
        }
        Preconditions.checkArgument(
                metadata.specs().size() == 1,
                "do not support clone iceberg table which had more than 1 partitionSpec."
                        + "table: %s, specs: %s",
                identifier.toString(),
                metadata.specs());

        org.apache.iceberg.io.FileIO icebergFileIO = new IcebergFileIO(fileIO);
        List<ManifestFile> dataManifests = currentSnapshot.dataManifests(icebergFileIO);
        List<ManifestFile> deleteManifests = currentSnapshot.deleteManifests(icebergFileIO);
        Preconditions.checkArgument(
                deleteManifests.isEmpty(),
                "do not support clone iceberg table which had 'DELETE' manifest file. "
                        + "table: %s, size of deleteManifests: %s.",
                identifier.toString(),
                deleteManifests.size());

        List<DataFile> dataFiles = readDataEntries(dataManifests, icebergFileIO);

        if (partitionRowType.getFieldCount() == 0) {
            // un-partition table
            return Collections.singletonList(toHivePartitionFiles(dataFiles, BinaryRow.EMPTY_ROW));
        } else {
            // partition table
            List<HivePartitionFiles> results = new ArrayList<>();
            List<BinaryWriter.ValueSetter> valueSetters = new ArrayList<>();
            partitionRowType
                    .getFieldTypes()
                    .forEach(type -> valueSetters.add(BinaryWriter.createValueSetter(type)));
            Map<StructLike, List<DataFile>> groupedDataFiles =
                    dataFiles.stream().collect(Collectors.groupingBy(DataFile::partition));
            for (Map.Entry<StructLike, List<DataFile>> entry : groupedDataFiles.entrySet()) {
                List<Object> partitionValues =
                        partitionToObjects(partitionRowType, (PartitionData) entry.getKey());
                BinaryRow partitionRow =
                        FileMetaUtils.writePartitionValue(
                                partitionRowType, partitionValues, valueSetters);
                if (predicate == null || predicate.test(partitionRow)) {
                    results.add(toHivePartitionFiles(entry.getValue(), partitionRow));
                }
            }
            return results;
        }
    }

    @Override
    public List<String> extractPartitionKeys(Table table) {
        String schemaJson = table.getParameters().get(CURRENT_SCHEMA);
        String specJson = table.getParameters().getOrDefault(DEFAULT_PARTITION_SPEC, "");
        if (specJson.isEmpty()) {
            return Collections.emptyList();
        } else {
            PartitionSpec spec =
                    PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaJson), specJson);
            return spec.fields().stream().map(PartitionField::name).collect(Collectors.toList());
        }
    }

    @Override
    public Map<String, String> extractOptions(Table table) {
        Map<String, String> hiveTableOptions = table.getParameters();
        Map<String, String> paimonOptions = new HashMap<>();
        String comment = hiveTableOptions.get("comment");
        if (comment != null) {
            paimonOptions.put("hive.comment", comment);
            paimonOptions.put("comment", comment);
        }

        String format =
                table.getParameters()
                        .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        paimonOptions.put(FILE_FORMAT.key(), format);
        Map<String, String> formatOptions =
                getIdentifierPrefixOptions("write." + format, hiveTableOptions);
        Map<String, String> sdFormatOptions =
                getIdentifierPrefixOptions(
                        "write." + format, table.getSd().getSerdeInfo().getParameters());
        // TODO: iceberg format options should be transformed to paimon options
        formatOptions.putAll(sdFormatOptions);
        paimonOptions.putAll(formatOptions);

        String compression = parseCompression(format, formatOptions);
        if (compression != null) {
            paimonOptions.put(FILE_COMPRESSION.key(), compression);
        }
        return paimonOptions;
    }

    private HivePartitionFiles toHivePartitionFiles(List<DataFile> dataFiles, BinaryRow partition) {
        List<org.apache.paimon.fs.Path> paths = new ArrayList<>(dataFiles.size());
        List<Long> fileSizes = new ArrayList<>(dataFiles.size());
        String format = null;
        for (DataFile file : dataFiles) {
            // note: file.path() will be deprecated in 2.0, file.location() was introduced in 1.7,
            // here using file.path() to be compatible with 1.7-
            org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(file.path().toString());
            if (format == null) {
                format = file.format().toString();
            }
            long fileSize = file.fileSizeInBytes();
            paths.add(path);
            fileSizes.add(fileSize);
        }
        return new HivePartitionFiles(partition, paths, fileSizes, format);
    }

    private List<DataFile> readDataEntries(
            List<ManifestFile> dataManifests, org.apache.iceberg.io.FileIO io) {
        List<DataFile> dateEntries = new ArrayList<>();
        for (ManifestFile dataManifest : dataManifests) {
            for (DataFile dataFile : ManifestFiles.read(dataManifest, io)) {
                dateEntries.add(dataFile);
            }
        }
        return dateEntries;
    }

    private List<Object> partitionToObjects(RowType partitionRowType, PartitionData partition) {
        Preconditions.checkArgument(partition.size() == partitionRowType.getFieldCount());
        List<Object> objects = new ArrayList<>();
        for (int i = 0; i < partition.size(); i++) {
            objects.add(partition.get(i));
        }
        return objects;
    }

    private String parseCompression(String format, Map<String, String> formatOptions) {
        String compression = null;
        if (Objects.equals(format, "avro")) {
            compression =
                    formatOptions.getOrDefault(
                            "write.avro.compression-codec", AVRO_COMPRESSION_DEFAULT);
        } else if (Objects.equals(format, "parquet")) {
            compression =
                    formatOptions.getOrDefault(
                            "write.parquet.compression-codec",
                            PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
        } else if (Objects.equals(format, "orc")) {
            compression =
                    formatOptions.getOrDefault(
                            "write.orc.compression-codec", ORC_COMPRESSION_DEFAULT);
        }
        return compression;
    }

    // -------------------------------------------------------------------------------------
    // Iceberg Class Wrapper
    // -------------------------------------------------------------------------------------

    private static class IcebergFileIO implements org.apache.iceberg.io.FileIO {
        private final FileIO fileIO;

        public IcebergFileIO(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InputFile newInputFile(String path) {

            return new InputFile() {
                @Override
                public long getLength() {
                    try {
                        return fileIO.getFileSize(new Path(path));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public SeekableInputStream newStream() {
                    try {
                        org.apache.paimon.fs.SeekableInputStream inputStream =
                                fileIO.newInputStream(new Path(path));
                        return new IcebergSeekableStreamAdapter(inputStream);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public String location() {
                    return path;
                }

                @Override
                public boolean exists() {
                    return true;
                }
            };
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }

    private static class IcebergSeekableStreamAdapter extends SeekableInputStream {
        private final org.apache.paimon.fs.SeekableInputStream paimonStream;

        IcebergSeekableStreamAdapter(org.apache.paimon.fs.SeekableInputStream paimonStream) {
            this.paimonStream = paimonStream;
        }

        @Override
        public long getPos() throws IOException {
            return paimonStream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            paimonStream.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            return paimonStream.read();
        }
    }
}
