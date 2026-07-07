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

package org.apache.paimon.format.shredding;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.shredding.MapSharedShreddingFieldMeta;
import org.apache.paimon.data.shredding.MapSharedShreddingUtils;
import org.apache.paimon.data.shredding.MapShreddingDefine;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcTypeUtil;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Format tests for shredding write plans. */
class ShreddingWritePlanFormatTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testParquetMapSharedShreddingFieldMetadataRoundtrip() throws Exception {
        FileFormat format =
                new ParquetFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        Map<String, Map<String, String>> fieldMetadata =
                writeAndReadFieldMetadata(format, "parquet", "none");

        assertMapSharedShreddingMetadata(fieldMetadata, "none");
        assertThat(fieldMetadata.get("id"))
                .containsEntry(FormatMetadataUtils.PARQUET_FIELD_ID_KEY, "0");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(FormatMetadataUtils.PARQUET_FIELD_ID_KEY, "1");
    }

    @Test
    void testOrcMapSharedShreddingFieldMetadataRoundtrip() throws Exception {
        FileFormat format =
                new OrcFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        Map<String, Map<String, String>> fieldMetadata =
                writeAndReadFieldMetadata(format, "orc", "none");

        assertMapSharedShreddingMetadata(fieldMetadata, "none");
        assertThat(fieldMetadata.get("id")).containsEntry(OrcTypeUtil.PAIMON_ORC_FIELD_ID_KEY, "0");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(OrcTypeUtil.PAIMON_ORC_FIELD_ID_KEY, "1");
    }

    private Map<String, Map<String, String>> writeAndReadFieldMetadata(
            FileFormat format, String suffix, String compression) throws IOException {
        FileIO fileIO = LocalFileIO.create();
        Path file = new Path(tempDir.resolve("data." + suffix).toUri().toString());
        RowType rowType = logicalRowType();

        FormatWriterFactory writerFactory =
                ShreddingWritePlanWriterFactories.wrapMapSharedShredding(
                        format.createWriterFactory(rowType), rowType, sharedShreddingOptions());
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = writerFactory.create(out, compression);
        writer.addElement(GenericRow.of(1, stringKeyMap("a", 10L, "b", 20L, "c", 30L)));
        writer.addElement(GenericRow.of(2, stringKeyMap("b", 40L, "d", 50L)));
        writer.close();
        out.close();

        FormatReaderContext readerContext =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        return ((SupportsFieldMetadata) format).readFieldMetadata(readerContext);
    }

    private static RowType logicalRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
    }

    private static CoreOptions sharedShreddingOptions() {
        Options options = new Options();
        options.setString("fields.tags.map.storage-layout", "shared-shredding");
        options.setString("fields.tags.map.shared-shredding.max-columns", "4");
        return new CoreOptions(options);
    }

    private static void assertMapSharedShreddingMetadata(
            Map<String, Map<String, String>> fieldMetadata, String compression) {
        assertThat(fieldMetadata).containsKey("tags");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(
                        MapShreddingDefine.STORAGE_LAYOUT,
                        MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);

        MapSharedShreddingFieldMeta fieldMeta =
                MapSharedShreddingUtils.deserializeMetadata(fieldMetadata.get("tags"), compression);
        assertThat(fieldMeta.nameToId())
                .containsEntry("a", 0)
                .containsEntry("b", 1)
                .containsEntry("c", 2)
                .containsEntry("d", 3);
        assertThat(fieldMeta.numColumns()).isEqualTo(4);
        assertThat(fieldMeta.maxRowWidth()).isEqualTo(3);
    }

    private static GenericMap stringKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(BinaryString.fromString((String) keyValues[i]), keyValues[i + 1]);
        }
        return new GenericMap(values);
    }
}
