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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleColStatsExtractorTest;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for {@link MosaicSimpleStatsExtractor}. */
class MosaicSimpleStatsExtractorTest extends SimpleColStatsExtractorTest {

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Override
    protected FileFormat createFormat() {
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Override
    protected RowType rowType() {
        return RowType.builder()
                .field("f_boolean", DataTypes.BOOLEAN())
                .field("f_tinyint", DataTypes.TINYINT())
                .field("f_smallint", DataTypes.SMALLINT())
                .field("f_int", DataTypes.INT())
                .field("f_bigint", DataTypes.BIGINT())
                .field("f_float", DataTypes.FLOAT())
                .field("f_double", DataTypes.DOUBLE())
                .field("f_string", DataTypes.STRING())
                .field("f_binary", DataTypes.BYTES())
                .field("f_decimal_5_2", DataTypes.DECIMAL(5, 2))
                .field("f_decimal_20_0", DataTypes.DECIMAL(20, 0))
                .field("f_date", DataTypes.DATE())
                .field("f_timestamp3", DataTypes.TIMESTAMP(3))
                .field("f_timestamp6", DataTypes.TIMESTAMP(6))
                .field("f_array", DataTypes.ARRAY(DataTypes.INT()))
                .build();
    }

    @Override
    protected String fileCompression() {
        return "zstd";
    }

    @Override
    protected SimpleColStats regenerate(SimpleColStats stats, DataType type) {
        if (type instanceof ArrayType) {
            return new SimpleColStats(null, null, stats.nullCount());
        }
        return stats;
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
