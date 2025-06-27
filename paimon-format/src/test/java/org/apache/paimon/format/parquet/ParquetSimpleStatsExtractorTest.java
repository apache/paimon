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

package org.apache.paimon.format.parquet;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleColStatsExtractorTest;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

/** Tests for {@link ParquetSimpleStatsExtractor}. */
public class ParquetSimpleStatsExtractorTest extends SimpleColStatsExtractorTest {

    @Override
    protected FileFormat createFormat() {
        return FileFormat.fromIdentifier("parquet", new Options());
    }

    @Override
    protected RowType rowType() {
        return RowType.builder()
                .fields(
                        new CharType(8),
                        new VarCharType(8),
                        new BooleanType(),
                        new BinaryType(8),
                        new VarBinaryType(8),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DecimalType(5, 2),
                        new DecimalType(15, 2),
                        new DecimalType(38, 18),
                        new DateType(),
                        new TimestampType(3),
                        new TimestampType(6),
                        new TimestampType(9),
                        new ArrayType(new IntType()),
                        new MapType(new VarCharType(8), new VarCharType(8)),
                        new MultisetType(new VarCharType(8)))
                .build();
    }

    @Override
    protected SimpleColStats regenerate(SimpleColStats stats, DataType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                if (timestampType.getPrecision() > 6) {
                    return new SimpleColStats(null, null, stats.nullCount());
                }
                break;
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                return new SimpleColStats(null, null, null);
        }
        return stats;
    }

    @Override
    protected String fileCompression() {
        return "SNAPPY";
    }
}
