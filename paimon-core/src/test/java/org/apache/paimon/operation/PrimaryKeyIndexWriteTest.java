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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.postpone.PostponeBucketFileStoreWrite;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests BTree and Bitmap primary-key index wiring through the file-store writer. */
class PrimaryKeyIndexWriteTest {

    @TempDir java.nio.file.Path tempDir;
    private IOManager ioManager;

    @BeforeEach
    void before() throws IOException {
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    @AfterEach
    void after() throws Exception {
        ioManager.close();
    }

    @Test
    void testCreatesCoordinatorForBTreeAndBitmapDefinitions() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "10");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "itemId");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "comment");
        TestFileStore store = createStore(options);
        KeyValueFileStoreWrite write = (KeyValueFileStoreWrite) store.newWrite();
        write.withIOManager(ioManager);
        TestKeyValueGenerator generator = new TestKeyValueGenerator();
        KeyValue record = generator.next();

        AbstractFileStoreWrite.WriterContainer<KeyValue> container =
                write.createWriterContainer(generator.getPartition(record), 1);

        assertThat(container.primaryKeyIndexMaintainer).isNotNull();
        assertThat(container.primaryKeyIndexMaintainer.buildNotCompleted()).isFalse();
        write.close();
    }

    @Test
    void testCreatesCoordinatorForMultipleScalarDefinitionsAndVector() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "10");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "itemId,score");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "comment,state");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.index.type", "hnsw");

        List<DataField> fields =
                new ArrayList<>(TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields());
        fields.add(new DataField(7, "score", DataTypes.BIGINT()));
        fields.add(new DataField(8, "state", DataTypes.INT()));
        fields.add(new DataField(9, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        TestFileStore store = createStore(options, new RowType(fields));
        KeyValueFileStoreWrite write = (KeyValueFileStoreWrite) store.newWrite();
        write.withIOManager(ioManager);
        TestKeyValueGenerator generator = new TestKeyValueGenerator();
        KeyValue record = generator.next();

        AbstractFileStoreWrite.WriterContainer<KeyValue> container =
                write.createWriterContainer(generator.getPartition(record), 1);

        assertThat(readField(container.primaryKeyIndexMaintainer, "vectorMaintainer")).isNotNull();
        assertThat((List<?>) readField(container.primaryKeyIndexMaintainer, "sortedMaintainers"))
                .hasSize(4);
        write.close();
    }

    @Test
    void testPostponeBucketDefersCoordinatorUntilFixedBucketCompaction() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), String.valueOf(BucketMode.POSTPONE_BUCKET));
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "itemId");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "comment");
        TableSchema schema = createSchema(options, TestKeyValueGenerator.DEFAULT_ROW_TYPE);
        TestKeyValueGenerator generator = new TestKeyValueGenerator();
        KeyValue record = generator.next();

        TestFileStore postponeStore =
                createStore(
                        schema, TestKeyValueGenerator.DEFAULT_ROW_TYPE, BucketMode.POSTPONE_BUCKET);
        AbstractFileStoreWrite<KeyValue> postponeWrite = postponeStore.newWrite();
        postponeWrite.withIOManager(ioManager);
        AbstractFileStoreWrite.WriterContainer<KeyValue> postponeContainer =
                postponeWrite.createWriterContainer(
                        generator.getPartition(record), BucketMode.POSTPONE_BUCKET);

        assertThat(postponeWrite).isInstanceOf(PostponeBucketFileStoreWrite.class);
        assertThat(postponeContainer.primaryKeyIndexMaintainer).isNull();
        postponeWrite.close();

        // Postpone compaction keeps the table schema but uses a fixed-bucket runtime view.
        TestFileStore compactStore = createStore(schema, TestKeyValueGenerator.DEFAULT_ROW_TYPE, 1);
        KeyValueFileStoreWrite compactWrite = (KeyValueFileStoreWrite) compactStore.newWrite();
        compactWrite.withIOManager(ioManager);
        AbstractFileStoreWrite.WriterContainer<KeyValue> compactContainer =
                compactWrite.createWriterContainer(generator.getPartition(record), 0);

        assertThat(compactContainer.primaryKeyIndexMaintainer).isNotNull();
        compactWrite.close();
    }

    private TestFileStore createStore(Map<String, String> options) throws Exception {
        return createStore(options, TestKeyValueGenerator.DEFAULT_ROW_TYPE);
    }

    private TestFileStore createStore(Map<String, String> options, RowType rowType)
            throws Exception {
        return createStore(createSchema(options, rowType), rowType, 10);
    }

    private TableSchema createSchema(Map<String, String> options, RowType rowType)
            throws Exception {
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        return schemaManager.createTable(
                new Schema(
                        rowType.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        options,
                        null));
    }

    private TestFileStore createStore(TableSchema schema, RowType rowType, int numBuckets) {
        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        numBuckets,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        rowType,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        schema)
                .build();
    }

    private static Object readField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
