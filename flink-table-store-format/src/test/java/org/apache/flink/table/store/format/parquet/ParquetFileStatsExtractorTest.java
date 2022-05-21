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

package org.apache.flink.table.store.format.parquet;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.stats.FileStatsExtractorTestBase;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

/** Tests for {@link ParquetFileStatsExtractor}. */
public class ParquetFileStatsExtractorTest extends FileStatsExtractorTestBase {

    @Override
    protected FileFormat createFormat() {
        return FileFormat.fromIdentifier(
                ParquetFileStatsExtractorTest.class.getClassLoader(),
                "parquet",
                new Configuration());
    }

    @Override
    protected RowType rowType() {
        return RowType.of(
                new CharType(8),
                new VarCharType(8),
                new BooleanType(),
                new TinyIntType(),
                new SmallIntType(),
                new IntType(),
                new BigIntType(),
                new FloatType(),
                new DoubleType(),
                new DecimalType(5, 2),
                new DecimalType(38, 18),
                new DateType());
    }
}
