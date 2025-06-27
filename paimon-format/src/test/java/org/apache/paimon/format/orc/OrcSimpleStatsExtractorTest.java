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

package org.apache.paimon.format.orc;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStatsExtractorTest;
import org.apache.paimon.format.orc.filter.OrcSimpleStatsExtractor;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

/** Tests for {@link OrcSimpleStatsExtractor}. */
public class OrcSimpleStatsExtractorTest extends SimpleColStatsExtractorTest {

    @Override
    protected FileFormat createFormat() {
        return FileFormat.fromIdentifier("orc", new Options());
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
                        new DecimalType(38, 18),
                        new DateType(),
                        new TimeType(),
                        new TimestampType(3),
                        new LocalZonedTimestampType(3),
                        // orc reader & writer currently cannot preserve a high precision timestamp
                        // new TimestampType(9),
                        new ArrayType(new IntType()),
                        new MapType(new VarCharType(8), new VarCharType(8)),
                        new MultisetType(new VarCharType(8)))
                .build();
    }

    @Override
    protected String fileCompression() {
        return "LZ4";
    }
}
