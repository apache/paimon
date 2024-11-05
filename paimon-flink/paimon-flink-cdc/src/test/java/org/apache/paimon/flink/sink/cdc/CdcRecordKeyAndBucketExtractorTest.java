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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.sink.RowDataKeyAndBucketExtractor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Tests for {@link CdcRecordKeyAndBucketExtractor}. */
public class CdcRecordKeyAndBucketExtractorTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.STRING(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.STRING()
                    },
                    new String[] {"pt1", "pt2", "k1", "v1", "k2", "v2"});

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testExtract() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableSchema schema = createTableSchema();
        RowDataKeyAndBucketExtractor expected = new RowDataKeyAndBucketExtractor(schema);
        CdcRecordKeyAndBucketExtractor actual = new CdcRecordKeyAndBucketExtractor(schema);

        int numTests = random.nextInt(1000) + 1;
        for (int i = 0; i < numTests; i++) {
            String pt1 = UUID.randomUUID().toString();
            int pt2 = random.nextInt();
            long k1 = random.nextLong();
            int v1 = random.nextInt();
            String k2 = UUID.randomUUID().toString();
            String v2 = UUID.randomUUID().toString();

            GenericRowData rowData =
                    GenericRowData.of(
                            StringData.fromString(pt1),
                            pt2,
                            k1,
                            v1,
                            StringData.fromString(k2),
                            StringData.fromString(v2));
            expected.setRecord(rowData);

            Map<String, String> data = new HashMap<>();
            data.put("pt1", pt1);
            data.put("pt2", String.valueOf(pt2));
            data.put("k1", String.valueOf(k1));
            data.put("v1", String.valueOf(v1));
            data.put("k2", k2);
            data.put("v2", v2);

            actual.setRecord(new CdcRecord(RowKind.INSERT, data));
            assertThat(actual.partition()).isEqualTo(expected.partition());
            assertThat(actual.bucket()).isEqualTo(expected.bucket());

            actual.setRecord(new CdcRecord(RowKind.DELETE, data));
            assertThat(actual.partition()).isEqualTo(expected.partition());
            assertThat(actual.bucket()).isEqualTo(expected.bucket());
        }
    }

    @Test
    public void testNullPartition() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableSchema schema = createTableSchema();
        RowDataKeyAndBucketExtractor expected = new RowDataKeyAndBucketExtractor(schema);
        CdcRecordKeyAndBucketExtractor actual = new CdcRecordKeyAndBucketExtractor(schema);

        long k1 = random.nextLong();
        int v1 = random.nextInt();
        String k2 = UUID.randomUUID().toString();
        String v2 = UUID.randomUUID().toString();

        GenericRowData rowData =
                GenericRowData.of(
                        null, null, k1, v1, StringData.fromString(k2), StringData.fromString(v2));
        expected.setRecord(rowData);

        Map<String, String> data = new HashMap<>();
        data.put("pt1", null);
        data.put("pt2", null);
        data.put("k1", String.valueOf(k1));
        data.put("v1", String.valueOf(v1));
        data.put("k2", k2);
        data.put("v2", v2);

        actual.setRecord(new CdcRecord(RowKind.INSERT, data));
        assertThat(actual.partition()).isEqualTo(expected.partition());
        assertThat(actual.bucket()).isEqualTo(expected.bucket());

        actual.setRecord(new CdcRecord(RowKind.DELETE, data));
        assertThat(actual.partition()).isEqualTo(expected.partition());
        assertThat(actual.bucket()).isEqualTo(expected.bucket());
    }

    @Test
    public void testEmptyPartition() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        TableSchema schema = createTableSchema();
        RowDataKeyAndBucketExtractor expected = new RowDataKeyAndBucketExtractor(schema);
        CdcRecordKeyAndBucketExtractor actual = new CdcRecordKeyAndBucketExtractor(schema);

        long k1 = random.nextLong();
        int v1 = random.nextInt();
        String k2 = UUID.randomUUID().toString();
        String v2 = UUID.randomUUID().toString();

        GenericRowData rowData =
                GenericRowData.of(
                        StringData.fromString(""),
                        null,
                        k1,
                        v1,
                        StringData.fromString(k2),
                        StringData.fromString(v2));
        expected.setRecord(rowData);

        Map<String, String> data = new HashMap<>();
        data.put("pt1", "");
        data.put("pt2", null);
        data.put("k1", String.valueOf(k1));
        data.put("v1", String.valueOf(v1));
        data.put("k2", k2);
        data.put("v2", v2);

        actual.setRecord(new CdcRecord(RowKind.INSERT, data));
        assertThat(actual.partition()).isEqualTo(expected.partition());
        assertThat(actual.bucket()).isEqualTo(expected.bucket());

        actual.setRecord(new CdcRecord(RowKind.DELETE, data));
        assertThat(actual.partition()).isEqualTo(expected.partition());
        assertThat(actual.bucket()).isEqualTo(expected.bucket());
    }

    private TableSchema createTableSchema() throws Exception {
        return SchemaUtils.forceCommit(
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString())),
                new Schema(
                        ROW_TYPE.getFields(),
                        Arrays.asList("pt1", "pt2"),
                        Arrays.asList("pt1", "pt2", "k1", "k2"),
                        Collections.singletonMap("bucket", "1"),
                        ""));
    }
}
