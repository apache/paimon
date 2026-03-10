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

package org.apache.paimon.format.lance;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import com.lancedb.lance.file.LanceFileReader;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with vector and lance file format. */
public class VectorTypeWithLanceTest extends TableTestBase {

    private final float[] testVector = randomVector();
    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private final Set<Integer> nullVectorIds = new HashSet<>();

    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString());
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toUri().toString());
        CatalogContext context = CatalogContext.create(options, new TraceableFileIO.Loader(), null);
        catalog = CatalogFactory.createCatalog(context);
        catalog.createDatabase(database, true);
    }

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        AtomicInteger integer = new AtomicInteger(0);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 10 == 0) {
                        boolean vectorIsNull = nullVectorIds.contains(row.getInt(0));
                        if (vectorIsNull) {
                            assertThat(row.isNullAt(2)).isEqualTo(true);
                        } else {
                            assertThat(row.isNullAt(2)).isEqualTo(false);
                            assertThat(row.getVector(2).toFloatArray()).isEqualTo(testVector);
                        }
                    }
                });

        assertThat(integer.get()).isEqualTo(100);

        FileStoreTable table = getTableDefault();
        RemoteIterator<FileStatus> files =
                table.fileIO().listFilesIterative(table.location(), true);
        while (files.hasNext()) {
            String file = files.next().getPath().toString();
            if (file.endsWith(".lance")) {
                checkFileByLanceReader(file);
                return;
            }
        }
        Assertions.fail("Do not find any lance file.");
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.VECTOR(testVector.length, DataTypes.FLOAT()));
        // schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), "lance");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        return schemaBuilder.build();
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        boolean shouldNull = RANDOM.nextBoolean();
        Integer id = idGenerator.getAndIncrement();
        BinaryVector vector = shouldNull ? null : BinaryVector.fromPrimitiveArray(testVector);
        if (shouldNull) {
            nullVectorIds.add(id);
        }
        return GenericRow.of(id, BinaryString.fromBytes(randomBytes()), vector);
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[RANDOM.nextInt(1024) + 1];
        RANDOM.nextBytes(binary);
        return binary;
    }

    private float[] randomVector() {
        byte[] randomBytes = randomBytes();
        float[] vector = new float[randomBytes.length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomBytes[i];
        }
        return vector;
    }

    private void checkFileByLanceReader(String path) throws Exception {
        ArrowType expected = new ArrowType.FixedSizeList(testVector.length);
        RootAllocator allocator = new RootAllocator();
        Map<String, String> options = new HashMap<>();
        try (LanceFileReader reader = LanceFileReader.open(path, options, allocator)) {
            org.apache.arrow.vector.types.pojo.Schema schema = reader.schema();
            org.apache.arrow.vector.types.pojo.Field field = schema.findField("f2");
            Assertions.assertEquals(expected, field.getFieldType().getType());
        }
    }
}
