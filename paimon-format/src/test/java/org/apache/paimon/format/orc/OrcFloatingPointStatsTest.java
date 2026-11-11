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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** ORC footer stats use primitive comparisons, which hide NaN and -0.0. */
class OrcFloatingPointStatsTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

    @Test
    public void testDoubleNaNIsNotExcludedByTheUpperBound() throws Exception {
        SimpleColStats stats = extract(DataTypes.DOUBLE(), new Double[] {1.0d, Double.NaN});

        assertThat(boundKeepsNaN(stats.max()))
                .as("max %s excludes a NaN that was written", stats.max())
                .isTrue();
    }

    @Test
    public void testFloatNaNIsNotExcludedByTheUpperBound() throws Exception {
        SimpleColStats stats = extract(DataTypes.FLOAT(), new Float[] {1.0f, Float.NaN});

        assertThat(boundKeepsNaN(stats.max()))
                .as("max %s excludes a NaN that was written", stats.max())
                .isTrue();
    }

    @Test
    public void testDoubleNegativeZeroIsNotExcludedByTheLowerBound() throws Exception {
        SimpleColStats stats = extract(DataTypes.DOUBLE(), new Double[] {0.0d, -0.0d});

        assertThat(boundKeepsNegativeZero(stats.min()))
                .as("min %s excludes a -0.0 that was written", stats.min())
                .isTrue();
    }

    @Test
    public void testFloatNegativeZeroIsNotExcludedByTheLowerBound() throws Exception {
        SimpleColStats stats = extract(DataTypes.FLOAT(), new Float[] {0.0f, -0.0f});

        assertThat(boundKeepsNegativeZero(stats.min()))
                .as("min %s excludes a -0.0 that was written", stats.min())
                .isTrue();
    }

    @Test
    public void testFiniteDoubleBoundsAreKept() throws Exception {
        SimpleColStats stats = extract(DataTypes.DOUBLE(), new Double[] {1.0d, 3.0d});

        assertThat(stats.min()).isEqualTo(1.0d);
        assertThat(stats.max()).isEqualTo(3.0d);
    }

    @Test
    public void testFiniteFloatBoundsAreKept() throws Exception {
        SimpleColStats stats = extract(DataTypes.FLOAT(), new Float[] {1.0f, 3.0f});

        assertThat(stats.min()).isEqualTo(1.0f);
        assertThat(stats.max()).isEqualTo(3.0f);
    }

    @Test
    public void testDoubleNegativeZeroUpperBoundDoesNotHidePositiveZero() throws Exception {
        SimpleColStats stats = extract(DataTypes.DOUBLE(), new Double[] {-1.0d, -0.0d, 0.0d});

        assertThat(stats.max()).as("max %s hides a +0.0 that was written", stats.max()).isNull();
    }

    @Test
    public void testFloatNegativeZeroUpperBoundDoesNotHidePositiveZero() throws Exception {
        SimpleColStats stats = extract(DataTypes.FLOAT(), new Float[] {-1.0f, -0.0f, 0.0f});

        assertThat(stats.max()).as("max %s hides a +0.0 that was written", stats.max()).isNull();
    }

    private SimpleColStats extract(DataType type, Object[] values) throws Exception {
        FileFormat format = FileFormat.fromIdentifier("orc", new Options());
        RowType rowType = RowType.of(type);
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        Path path = new Path(tempDir.toString() + "/stats.orc");
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, "LZ4");
        for (Object value : values) {
            writer.addElement(GenericRow.of(value));
        }
        writer.close();

        SimpleColStatsCollector.Factory[] collectors =
                new SimpleColStatsCollector.Factory[] {SimpleColStatsCollector.from("full")};
        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        return extractor.extract(fileIO, path, fileIO.getFileSize(path))[0];
    }

    /** A missing bound cannot skip the file. A NaN bound is the greatest value. */
    private static boolean boundKeepsNaN(Object bound) {
        if (bound == null) {
            return true;
        }
        if (bound instanceof Double) {
            return Double.isNaN((Double) bound);
        }
        if (bound instanceof Float) {
            return Float.isNaN((Float) bound);
        }
        return false;
    }

    /** A missing bound cannot skip the file. Otherwise the bound must be negative zero. */
    private static boolean boundKeepsNegativeZero(Object bound) {
        if (bound == null) {
            return true;
        }
        if (bound instanceof Double) {
            return Double.doubleToRawLongBits((Double) bound) == Double.doubleToRawLongBits(-0.0d);
        }
        if (bound instanceof Float) {
            return Float.floatToRawIntBits((Float) bound) == Float.floatToRawIntBits(-0.0f);
        }
        return false;
    }
}
