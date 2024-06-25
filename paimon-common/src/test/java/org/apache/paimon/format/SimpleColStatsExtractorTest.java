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

package org.apache.paimon.format;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SimpleStatsExtractor}. */
public abstract class SimpleColStatsExtractorTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

    @ParameterizedTest
    @ValueSource(strings = {"none", "counts", "full", "truncate(3)", "truncate(12)"})
    public void testExtract(String mode) throws Exception {
        FileFormat format = createFormat();
        RowType rowType = rowType();
        int count = rowType().getFieldCount();
        SimpleColStatsCollector.Factory[] stats =
                IntStream.range(0, count)
                        .mapToObj(p -> SimpleColStatsCollector.from(mode))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        Path path = new Path(tempDir.toString() + "/test");
        PositionOutputStream out = new LocalFileIO().newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, fileCompression());

        List<GenericRow> data = createData(rowType);
        for (GenericRow row : data) {
            writer.addElement(row);
        }
        writer.finish();

        SimpleStatsCollector collector = new SimpleStatsCollector(rowType, stats);
        for (GenericRow row : data) {
            collector.collect(row);
        }
        SimpleColStats[] expected = collector.extract();

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, stats).get();
        assertThat(extractor).isNotNull();
        SimpleColStats[] actual = extractor.extract(fileIO, path);
        for (int i = 0; i < expected.length; i++) {
            expected[i] = regenerate(expected[i], rowType.getTypeAt(i));
        }
        assertThat(actual).isEqualTo(expected);
    }

    protected SimpleColStats regenerate(SimpleColStats stats, DataType type) {
        return stats;
    }

    private List<GenericRow> createData(RowType rowType) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numRows = random.nextInt(1, 100);
        List<List<Object>> columns = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            List<Object> column = new ArrayList<>();
            int numValues = random.nextInt(numRows + 1);
            for (int i = 0; i < numValues; i++) {
                column.add(createField(field.type()));
            }
            columns.add(column);
        }
        return createData(numRows, columns);
    }

    private List<GenericRow> createData(int numRows, List<List<Object>> columns) {
        List<GenericRow> rows = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            rows.add(new GenericRow(columns.size()));
        }
        for (int i = 0; i < columns.size(); i++) {
            List<?> objects = new ArrayList<>(columns.get(i));
            while (objects.size() < numRows) {
                objects.add(null);
            }
            Collections.shuffle(objects);
            for (int j = 0; j < numRows; j++) {
                rows.get(j).setField(i, objects.get(j));
            }
        }
        return rows;
    }

    private Object createField(DataType type) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        switch (type.getTypeRoot()) {
            case CHAR:
                CharType charType = (CharType) type;
                return BinaryString.fromString(randomString(charType.getLength()));
            case VARCHAR:
                VarCharType varCharType = (VarCharType) type;
                return BinaryString.fromString(
                        randomString(random.nextInt(varCharType.getLength()) + 1));
            case BOOLEAN:
                return random.nextBoolean();
            case BINARY:
                BinaryType binaryType = (BinaryType) type;
                return randomString(binaryType.getLength()).getBytes();
            case VARBINARY:
                VarBinaryType varBinaryType = (VarBinaryType) type;
                return randomString(varBinaryType.getLength()).getBytes();
            case TINYINT:
                return (byte) random.nextInt(10);
            case SMALLINT:
                return (short) random.nextInt(100);
            case INTEGER:
                return random.nextInt(1000);
            case BIGINT:
                return random.nextLong(10000);
            case FLOAT:
                return random.nextFloat();
            case DOUBLE:
                return random.nextDouble();
            case DECIMAL:
                return randomDecimalData((DecimalType) type);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return random.nextInt(10000);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return randomTimestampData(getPrecision(type));
            case ARRAY:
                return randomArray((ArrayType) type);
            case MAP:
                return randomMap((MapType) type);
            case MULTISET:
                return randomMultiset((MultisetType) type);
            default:
                throw new UnsupportedOperationException("Unsupported type " + type.asSQLString());
        }
    }

    private String randomString(int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append((char) ThreadLocalRandom.current().nextInt(127 - 32));
        }
        return builder.toString();
    }

    private Decimal randomDecimalData(DecimalType type) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int p = type.getPrecision();
        int s = type.getScale();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < p - s; i++) {
            builder.append((char) (random.nextInt(10) + '0'));
        }
        if (s > 0) {
            builder.append('.');
            for (int i = 0; i < s; i++) {
                builder.append((char) (random.nextInt(10) + '0'));
            }
        }
        return Decimal.fromBigDecimal(new BigDecimal(builder.toString()), p, s);
    }

    private Timestamp randomTimestampData(int precision) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long p = 1;
        for (int i = precision; i < TimestampType.MAX_PRECISION; i++) {
            p *= 10;
        }
        long currentSecond = System.currentTimeMillis() / 1000;
        return Timestamp.fromInstant(
                Instant.ofEpochSecond(
                        random.nextLong(currentSecond), random.nextLong(1_000_000_000) / p * p));
    }

    private InternalArray randomArray(ArrayType type) {
        int length = ThreadLocalRandom.current().nextInt(10);
        Object[] javaArray = new Object[length];
        for (int i = 0; i < length; i++) {
            javaArray[i] = createField(type.getElementType());
        }
        return new GenericArray(javaArray);
    }

    private InternalMap randomMap(MapType type) {
        int length = ThreadLocalRandom.current().nextInt(10);
        Map<Object, Object> javaMap = new HashMap<>(length);
        for (int i = 0; i < length; i++) {
            javaMap.put(createField(type.getKeyType()), createField(type.getValueType()));
        }
        return new GenericMap(javaMap);
    }

    private InternalMap randomMultiset(MultisetType type) {
        int length = ThreadLocalRandom.current().nextInt(10);
        Map<Object, Object> javaMap = new HashMap<>(length);
        IntType intType = new IntType(false);
        for (int i = 0; i < length; i++) {
            javaMap.put(createField(type.getElementType()), createField(intType));
        }
        return new GenericMap(javaMap);
    }

    protected abstract FileFormat createFormat();

    protected abstract RowType rowType();

    protected abstract String fileCompression();
}
