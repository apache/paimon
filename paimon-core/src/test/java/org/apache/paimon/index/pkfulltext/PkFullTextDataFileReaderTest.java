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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.options.MemorySize.VALUE_128_MB;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests reading physical text positions from compact data files. */
class PkFullTextDataFileReaderTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testReadsProjectedTextAndPreservesNullPositions() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        DataField keyField = new DataField(0, "id", DataTypes.INT());
        DataField textField = new DataField(1, "content", DataTypes.STRING());
        DataField payloadField = new DataField(2, "payload", DataTypes.INT());
        RowType keyType = RowType.of(keyField);
        RowType valueType = RowType.of(textField, payloadField);
        List<DataField> fields = Arrays.asList(keyField, textField, payloadField);
        TableSchema schema =
                new TableSchema(
                        0,
                        fields,
                        2,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.emptyMap(),
                        "");
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        assertThat(schemaManager.commit(schema)).isTrue();
        FileStorePathFactory pathFactory = createNonPartFactory(tablePath);
        FlushingFileFormat format = new FlushingFileFormat("avro");
        CoreOptions options = new CoreOptions(new Options());

        KeyValueFileWriterFactory writerFactory =
                KeyValueFileWriterFactory.builder(
                                fileIO,
                                schema.id(),
                                keyType,
                                valueType,
                                format,
                                ignored -> pathFactory,
                                VALUE_128_MB.getBytes())
                        .build(BinaryRow.EMPTY_ROW, 0, options);
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(1, FileSource.COMPACT);
        try {
            writer.write(keyValue(1, "first", 10));
            writer.write(keyValue(2, null, 20));
            writer.write(keyValue(3, "third", 30));
        } finally {
            writer.close();
        }
        DataFileMeta dataFile = writer.result().get(0);

        KeyValueFileReaderFactory.Builder readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schema,
                        keyType,
                        valueType,
                        ignored -> format,
                        pathFactory,
                        fieldsExtractor(keyType, valueType),
                        options);
        PkFullTextDataFileReader reader =
                new PkFullTextDataFileReader.Factory(
                                readerFactoryBuilder, BinaryRow.EMPTY_ROW, 0, textField)
                        .create(dataFile);
        try {
            assertThat(reader.rowCount()).isEqualTo(3);
            assertThat(reader.readNextText()).hasToString("first");
            assertThat(reader.readNextText()).isNull();
            assertThat(reader.readNextText()).hasToString("third");
            assertThatThrownBy(reader::readNextText).hasMessageContaining("Read past data file");
        } finally {
            reader.close();
        }
    }

    private static KeyValue keyValue(int key, String text, int payload) {
        return new KeyValue()
                .replace(
                        GenericRow.of(key),
                        RowKind.INSERT,
                        GenericRow.of(
                                text == null ? null : BinaryString.fromString(text), payload));
    }

    private static KeyValueFieldsExtractor fieldsExtractor(RowType keyType, RowType valueType) {
        return new KeyValueFieldsExtractor() {
            @Override
            public List<DataField> keyFields(TableSchema schema) {
                return keyType.getFields();
            }

            @Override
            public List<DataField> valueFields(TableSchema schema) {
                return valueType.getFields();
            }
        };
    }
}
