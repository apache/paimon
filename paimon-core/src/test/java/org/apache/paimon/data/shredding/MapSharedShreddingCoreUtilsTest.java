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

package org.apache.paimon.data.shredding;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapSharedShreddingCoreUtils}. */
public class MapSharedShreddingCoreUtilsTest {

    private static final RowType WRITE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(0, "id", DataTypes.INT()),
                    DataTypes.FIELD(
                            1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                    DataTypes.FIELD(
                            2, "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testNoShreddingColumnsReturnsNull() {
        MapSharedShreddingContext context =
                MapSharedShreddingCoreUtils.createAndRestoreContext(
                        WRITE_TYPE,
                        Collections.emptyList(),
                        testPathFactory(),
                        new CoreOptions(new Options()),
                        null);

        assertThat(context).isNull();
    }

    @Test
    public void testNoRestoredFilesUsesConfiguredMaxColumns() {
        MapSharedShreddingContext context =
                MapSharedShreddingCoreUtils.createAndRestoreContext(
                        WRITE_TYPE,
                        Collections.emptyList(),
                        testPathFactory(),
                        options("tags", 128),
                        null);

        assertThat(context).isNotNull();
        assertThat(context.computeNextK()).containsEntry("tags", 128);
    }

    @Test
    public void testRestoreLatestFieldMetadata() throws IOException {
        DataFilePathFactory pathFactory = testPathFactory();
        FileIO fileIO = LocalFileIO.create();
        DataFileMeta olderFile =
                writeParquetFile(
                        pathFactory, fileIO, "older.parquet", null, schemaWithField("tags", 7));
        DataFileMeta newerFile =
                writeParquetFile(
                        pathFactory, fileIO, "newer.parquet", null, schemaWithField("tags", 23));

        MapSharedShreddingContext context =
                MapSharedShreddingCoreUtils.createAndRestoreContext(
                        WRITE_TYPE,
                        Arrays.asList(olderFile, newerFile),
                        pathFactory,
                        options("tags", 128),
                        fileIO);

        assertThat(context).isNotNull();
        assertThat(context.computeNextK()).containsEntry("tags", 23);
    }

    @Test
    public void testRestoreMultipleFieldsIndependently() throws IOException {
        DataFilePathFactory pathFactory = testPathFactory();
        FileIO fileIO = LocalFileIO.create();
        DataFileMeta tagsFile =
                writeParquetFile(
                        pathFactory,
                        fileIO,
                        "tags.parquet",
                        Collections.singletonList("tags"),
                        schemaWithField("tags", 17));
        DataFileMeta attrsFile =
                writeParquetFile(
                        pathFactory,
                        fileIO,
                        "attrs.parquet",
                        Collections.singletonList("attrs"),
                        schemaWithField("attrs", 9));

        MapSharedShreddingContext context =
                MapSharedShreddingCoreUtils.createAndRestoreContext(
                        WRITE_TYPE,
                        Arrays.asList(tagsFile, attrsFile),
                        pathFactory,
                        options(Arrays.asList("tags", "attrs"), Arrays.asList(128, 64)),
                        fileIO);

        assertThat(context).isNotNull();
        assertThat(context.computeNextK()).containsEntry("tags", 17).containsEntry("attrs", 9);
    }

    @Test
    public void testWriteColsFilterSkipsUnrelatedFiles() throws IOException {
        DataFilePathFactory pathFactory = testPathFactory();
        FileIO fileIO = LocalFileIO.create();
        DataFileMeta tagsFile =
                writeParquetFile(
                        pathFactory,
                        fileIO,
                        "tags.parquet",
                        Collections.singletonList("tags"),
                        schemaWithField("tags", 19));

        MapSharedShreddingContext context =
                MapSharedShreddingCoreUtils.createAndRestoreContext(
                        WRITE_TYPE,
                        Arrays.asList(
                                tagsFile, dataFile("id.parquet", Collections.singletonList("id"))),
                        pathFactory,
                        options("tags", 128),
                        fileIO);

        assertThat(context).isNotNull();
        assertThat(context.computeNextK()).containsEntry("tags", 19);
    }

    @Test
    public void testArrowSchemaWithoutSharedShreddingMetadataIsSkipped() throws IOException {
        DataFilePathFactory pathFactory = testPathFactory();
        FileIO fileIO = LocalFileIO.create();
        byte[] schema =
                FormatMetadataUtils.buildArrowSchemaMetadata(
                        WRITE_TYPE,
                        Collections.singletonMap(
                                "tags", Collections.singletonMap("custom.key", "value")),
                        FormatMetadataUtils.PARQUET_FIELD_ID_KEY);
        DataFileMeta file =
                writeParquetFile(pathFactory, fileIO, "plain-metadata.parquet", null, schema);

        MapSharedShreddingContext context =
                MapSharedShreddingCoreUtils.createAndRestoreContext(
                        WRITE_TYPE,
                        Collections.singletonList(file),
                        pathFactory,
                        options("tags", 128),
                        fileIO);

        assertThat(context).isNotNull();
        assertThat(context.computeNextK()).containsEntry("tags", 128);
    }

    private static CoreOptions options(String fieldName, int maxColumns) {
        return options(Collections.singletonList(fieldName), Collections.singletonList(maxColumns));
    }

    private static CoreOptions options(List<String> fieldNames, List<Integer> maxColumns) {
        Options options = new Options();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            options.setString("fields." + fieldName + ".map.storage-layout", "shared-shredding");
            options.setString(
                    "fields." + fieldName + ".map.shared-shredding.max-columns",
                    String.valueOf(maxColumns.get(i)));
        }
        return new CoreOptions(options);
    }

    private static byte[] schemaWithField(String fieldName, int maxRowWidth) {
        return schemaWithField(WRITE_TYPE, fieldName, maxRowWidth);
    }

    private static byte[] schemaWithField(RowType rowType, String fieldName, int maxRowWidth) {
        return FormatMetadataUtils.buildArrowSchemaMetadata(
                rowType,
                Collections.singletonMap(fieldName, fieldMetadata(maxRowWidth)),
                FormatMetadataUtils.PARQUET_FIELD_ID_KEY);
    }

    private static Map<String, String> fieldMetadata(int maxRowWidth) {
        Map<String, Integer> nameToId = new TreeMap<>();
        nameToId.put("k", 0);

        Map<Integer, List<Integer>> fieldToColumns = new TreeMap<>();
        fieldToColumns.put(0, Collections.singletonList(0));

        Set<Integer> overflowFieldSet = new TreeSet<>();
        Map<String, String> metadata = new LinkedHashMap<>();
        MapSharedShreddingUtils.serializeMetadata(
                new MapSharedShreddingFieldMeta(
                        nameToId, fieldToColumns, overflowFieldSet, 128, maxRowWidth),
                MapSharedShreddingDefine.DEFAULT_DICT_COMPRESSION,
                metadata);
        return metadata;
    }

    private static DataFileMeta dataFile(String fileName, @Nullable List<String> writeCols) {
        return dataFile(fileName, writeCols, 10L, 0L);
    }

    private static DataFileMeta dataFile(
            String fileName, @Nullable List<String> writeCols, long fileSize, long schemaId) {
        return DataFileMeta.forAppend(
                fileName,
                fileSize,
                1L,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                schemaId,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                writeCols);
    }

    private static DataFileMeta writeParquetFile(
            DataFilePathFactory pathFactory,
            FileIO fileIO,
            String fileName,
            @Nullable List<String> writeCols,
            byte[] arrowSchema)
            throws IOException {
        Path path = pathFactory.toPath(dataFile(fileName, null));
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        FormatWriterFactory writerFactory = format.createWriterFactory(WRITE_TYPE);
        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer = writerFactory.create(out, "zstd")) {
            ((SupportsWriterMetadata) writer)
                    .addMetadata(
                            Collections.singletonMap(
                                    FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY, arrowSchema));
            writer.addElement(
                    GenericRow.of(
                            1,
                            new GenericMap(
                                    Collections.singletonMap(BinaryString.fromString("k"), 1L)),
                            new GenericMap(
                                    Collections.singletonMap(BinaryString.fromString("k"), 1))));
        }
        return dataFile(fileName, writeCols, fileIO.getFileSize(path), 0L);
    }

    private DataFilePathFactory testPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir.toUri()), "parquet", "data-", "changelog-", false, "none", null);
    }
}
