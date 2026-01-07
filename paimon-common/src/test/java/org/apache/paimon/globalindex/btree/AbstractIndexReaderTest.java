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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.DecimalUtils;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Common test class for BTreeIndexReader. */
public class AbstractIndexReaderTest {
    protected static final CacheManager CACHE_MANAGER = new CacheManager(MemorySize.VALUE_8_MB);

    protected DataType dataType;
    protected int dataNum;
    protected List<Pair<Object, Long>> data;
    protected KeySerializer keySerializer;
    protected Comparator<Object> comparator;
    protected FileIO fileIO;
    protected GlobalIndexFileReader fileReader;
    protected GlobalIndexFileWriter fileWriter;
    protected BTreeGlobalIndexer globalIndexer;
    protected Options options;

    @TempDir protected java.nio.file.Path tempPath;

    AbstractIndexReaderTest(List<Object> args) {
        this.dataType = (DataType) args.get(0);
        this.dataNum = (Integer) args.get(1);
    }

    public void setUp() throws Exception {
        fileIO = LocalFileIO.create();
        fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "test-btree-" + UUID.randomUUID() + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(
                                new Path(new Path(tempPath.toUri()), fileName), true);
                    }
                };
        fileReader =
                new GlobalIndexFileReader() {
                    @Override
                    public SeekableInputStream getInputStream(String fileName) throws IOException {
                        return fileIO.newInputStream(
                                new Path(new Path(tempPath.toUri()), fileName));
                    }

                    @Override
                    public Path filePath(String fileName) {
                        return new Path(new Path(tempPath.toUri()), fileName);
                    }
                };
        options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE, MemorySize.ofMebiBytes(8));
        globalIndexer = new BTreeGlobalIndexer(new DataField(1, "testField", dataType), options);
        keySerializer = KeySerializer.create(dataType);
        comparator = keySerializer.createComparator();

        // generate data and sort by key
        data = new ArrayList<>(dataNum);
        DataGenerator dataGenerator = new DataGenerator();
        for (int i = 0; i < dataNum; i++) {
            data.add(Pair.of(dataType.accept(dataGenerator), (long) i));
        }
        data.sort((p1, p2) -> comparator.compare(p1.getKey(), p2.getKey()));
    }

    protected GlobalIndexIOMeta writeData(List<Pair<Object, Long>> data) throws IOException {
        GlobalIndexParallelWriter indexWriter = globalIndexer.createWriter(fileWriter);
        for (Pair<Object, Long> pair : data) {
            indexWriter.write(pair.getKey(), pair.getValue());
        }
        List<ResultEntry> results = indexWriter.finish();
        Assertions.assertEquals(1, results.size());

        ResultEntry resultEntry = results.get(0);
        String fileName = resultEntry.fileName();
        return new GlobalIndexIOMeta(
                fileName,
                fileIO.getFileSize(new Path(new Path(tempPath.toUri()), fileName)),
                resultEntry.meta());
    }

    protected List<Long> filter(Predicate<Object> filter) {
        return data.stream()
                .filter(pair -> filter.test(pair.getKey()))
                .map(Pair::getValue)
                .collect(Collectors.toList());
    }

    protected void assertResult(GlobalIndexResult indexResult, List<Long> expected) {
        Iterator<Long> iter = indexResult.results().iterator();
        List<Long> result = new ArrayList<>();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** The Generator to generate test data. */
    private static class DataGenerator extends DataTypeDefaultVisitor<Object> {
        Random random = new Random();

        @Override
        public Object visit(CharType charType) {
            return BinaryString.fromString("random char " + random.nextInt());
        }

        @Override
        public Object visit(VarCharType varCharType) {
            return BinaryString.fromString("random varchar " + random.nextInt());
        }

        @Override
        public Object visit(BooleanType booleanType) {
            return random.nextBoolean();
        }

        @Override
        public Object visit(DecimalType decimalType) {
            return DecimalUtils.castFrom(
                    random.nextDouble(), decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public Object visit(TinyIntType tinyIntType) {
            return (byte) random.nextInt();
        }

        @Override
        public Object visit(SmallIntType smallIntType) {
            return (short) random.nextInt();
        }

        @Override
        public Object visit(IntType intType) {
            return random.nextInt();
        }

        @Override
        public Object visit(BigIntType bigIntType) {
            return random.nextLong();
        }

        @Override
        public Object visit(FloatType floatType) {
            return random.nextFloat();
        }

        @Override
        public Object visit(DoubleType doubleType) {
            return random.nextDouble();
        }

        @Override
        public Object visit(DateType dateType) {
            return random.nextInt(Integer.MAX_VALUE);
        }

        @Override
        public Object visit(TimeType timeType) {
            return random.nextInt(Integer.MAX_VALUE);
        }

        @Override
        public Object visit(TimestampType timestampType) {
            return Timestamp.fromMicros(random.nextLong());
        }

        @Override
        public Object visit(LocalZonedTimestampType localZonedTimestampType) {
            return LocalZoneTimestamp.fromMicros(random.nextLong());
        }

        @Override
        protected Object defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                    "Btree index do not support " + dataType + " type.");
        }
    }
}
