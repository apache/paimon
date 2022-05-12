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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.PartitionedManifestMeta;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileStoreSource}. */
public class FileStoreSourceTest {

    private static final RowType RECORD_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new IntType(), new VarCharType(), new DoubleType(), new VarCharType()
                    },
                    new String[] {"k0", "k1", "v0", "v1"});

    @MethodSource("parameters")
    @ParameterizedTest
    public void testSerDe(boolean hasPk, boolean partitioned, boolean specified)
            throws ClassNotFoundException, IOException {
        FileStore fileStore = buildFileStore(hasPk, partitioned);
        Long specifiedSnapshotId = specified ? 1L : null;
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> specifiedManifestEntries =
                specified ? buildManifestEntries(hasPk, partitioned) : null;
        PartitionedManifestMeta partitionedManifestMeta =
                specified
                        ? new PartitionedManifestMeta(specifiedSnapshotId, specifiedManifestEntries)
                        : null;
        FileStoreSource source =
                new FileStoreSource(
                        fileStore,
                        !hasPk,
                        true,
                        Duration.ofSeconds(1).toMillis(),
                        true,
                        null,
                        null,
                        null,
                        partitionedManifestMeta);
        Object object = readObject(writeObject(source));
        assertThat(object).isInstanceOf(FileStoreSource.class);
        FileStoreSource deserialized = (FileStoreSource) object;
        assertThat(deserialized.getBoundedness()).isEqualTo(source.getBoundedness());
        if (specified) {
            assertThat(deserialized.getSpecifiedPartManifests())
                    .isEqualTo(source.getSpecifiedPartManifests());
        } else {
            assertThat(deserialized.getSpecifiedPartManifests()).isNull();
        }
    }

    private byte[] writeObject(FileStoreSource source) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(source);
        oos.close();
        return baos.toByteArray();
    }

    private Object readObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object object = ois.readObject();
        ois.close();
        return object;
    }

    public static Stream<Arguments> parameters() {
        // hasPk, partitioned, specified
        return Stream.of(
                Arguments.of(true, true, false),
                Arguments.of(true, false, false),
                Arguments.of(false, false, false),
                Arguments.of(false, true, false),
                Arguments.of(true, true, true),
                Arguments.of(true, false, true),
                Arguments.of(false, false, true),
                Arguments.of(false, true, true));
    }

    private static FileStore buildFileStore(boolean hasPk, boolean partitioned) {
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat", "db", "tbl");
        FileStoreOptions options = new FileStoreOptions(new Configuration());
        String user = "user";
        RowType partitionType = getPartitionType(partitioned);
        RowType keyType = getKeyType(hasPk);
        RowType valueType = getValueType(hasPk);
        MergeFunction mergeFunction =
                hasPk ? new DeduplicateMergeFunction() : new ValueCountMergeFunction();
        return new FileStoreImpl(
                options.path(tableIdentifier).toString(),
                options,
                user,
                partitionType,
                keyType,
                valueType,
                mergeFunction);
    }

    private static Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> buildManifestEntries(
            boolean hasPk, boolean partitioned) {
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> manifestEntries = new HashMap<>();
        Map<Integer, List<DataFileMeta>> bucketEntries = new HashMap<>();
        int totalBuckets = new Random().nextInt(10) + 1;
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            List<DataFileMeta> metaList = new ArrayList<>();
            for (Tuple2<BinaryRowData, BinaryRowData> tuple : genMinMax(hasPk)) {
                metaList.add(genDataFileMeta(hasPk, tuple.f0, tuple.f1));
            }
            bucketEntries.put(bucket, metaList);
        }
        genPartitionValues(partitioned)
                .forEach(partValue -> manifestEntries.put(partValue, bucketEntries));
        return manifestEntries;
    }

    private static List<BinaryRowData> genPartitionValues(boolean partitioned) {
        RowDataSerializer partSerializer = new RowDataSerializer(getPartitionType(partitioned));
        if (partitioned) {
            int partSize = new Random().nextInt(10) + 1;
            List<BinaryRowData> partKeys = new ArrayList<>();
            for (int i = 0; i < partSize; i++) {
                partKeys.add(
                        partSerializer
                                .toBinaryRow(
                                        GenericRowData.of(
                                                StringData.fromString(
                                                        UUID.randomUUID().toString())))
                                .copy());
            }
            return partKeys;
        }
        return Collections.singletonList(partSerializer.toBinaryRow(GenericRowData.of()).copy());
    }

    private static List<Tuple2<BinaryRowData, BinaryRowData>> genMinMax(boolean hasPk) {
        RowDataSerializer keySerializer = new RowDataSerializer(getKeyType(hasPk));
        int size = new Random().nextInt(20);
        List<Tuple2<BinaryRowData, BinaryRowData>> minMaxKeys = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            int k0 = new Random().nextInt(1000);
            String k1 = UUID.randomUUID().toString();
            BinaryRowData keyMin;
            BinaryRowData keyMax;
            if (hasPk) {
                keyMin =
                        keySerializer.toBinaryRow(GenericRowData.of(k0, StringData.fromString(k1)));
                keyMax =
                        keySerializer.toBinaryRow(
                                GenericRowData.of(k0 + 100, StringData.fromString(k1)));
            } else {
                double v0 = new Random().nextDouble();
                String v1 = UUID.randomUUID().toString();
                keyMin =
                        keySerializer.toBinaryRow(
                                GenericRowData.of(
                                        k0,
                                        StringData.fromString(k1),
                                        v0,
                                        StringData.fromString(v1)));
                keyMax =
                        keySerializer.toBinaryRow(
                                GenericRowData.of(
                                        k0 + 100,
                                        StringData.fromString(k1),
                                        v0 + 1.5,
                                        StringData.fromString(v1)));
            }
            minMaxKeys.add(Tuple2.of(keyMin, keyMax));
        }
        return minMaxKeys;
    }

    private static DataFileMeta genDataFileMeta(
            boolean hasPk, BinaryRowData keyMin, BinaryRowData keyMax) {
        long seqNumber = new Random().nextLong() + 1;
        FieldStats k0Stats = new FieldStats(keyMin.getInt(0), keyMax.getInt(1), 0);
        FieldStats k1Stats = new FieldStats(keyMin.getString(1), keyMax.getString(1), 0);
        FieldStats v0Status =
                new FieldStats(
                        hasPk ? null : keyMin.getDouble(2), hasPk ? null : keyMax.getDouble(2), 0);
        FieldStats v1Status =
                new FieldStats(
                        hasPk ? null : keyMin.getString(3), hasPk ? null : keyMax.getString(3), 0);
        long count = new Random().nextLong();

        FieldStatsArraySerializer keyStatsSer = new FieldStatsArraySerializer(getKeyType(hasPk));
        FieldStatsArraySerializer valueStatsSer =
                new FieldStatsArraySerializer(getValueType(hasPk));

        FieldStats[] keyStats =
                hasPk
                        ? new FieldStats[] {k0Stats, k1Stats}
                        : new FieldStats[] {
                            k0Stats, k1Stats,
                            v0Status, v1Status
                        };
        FieldStats[] valueStats =
                hasPk
                        ? new FieldStats[] {
                            k0Stats, k1Stats,
                            v0Status, v1Status
                        }
                        : new FieldStats[] {
                            new FieldStats(count, count + new Random().nextInt(100), 0)
                        };
        return new DataFileMeta(
                "data-" + UUID.randomUUID(),
                new Random().nextInt(100),
                new Random().nextInt(100),
                keyMin,
                keyMax,
                keyStatsSer.toBinary(keyStats),
                valueStatsSer.toBinary(valueStats),
                seqNumber,
                seqNumber + new Random().nextInt(100),
                new Random().nextInt(4));
    }

    private static RowType getKeyType(boolean hasPk) {
        return hasPk
                ? RowType.of(
                        new LogicalType[] {new IntType(), new VarCharType()},
                        new String[] {"k0", "k1"})
                : RECORD_TYPE;
    }

    private static RowType getValueType(boolean hasPk) {
        return hasPk
                ? RECORD_TYPE
                : RowType.of(
                        new LogicalType[] {new BigIntType(false)}, new String[] {"_VALUE_COUNT"});
    }

    private static RowType getPartitionType(boolean partitioned) {
        return partitioned
                ? RowType.of(new LogicalType[] {new VarCharType()}, new String[] {"k1"})
                : RowType.of();
    }
}
