/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.format.parquet;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ParquetFileFormatFactory}. */
public class ParquetFileFormatTest {
    private static final ConfigOption<String> KEY1 =
            ConfigOptions.key("k1").stringType().defaultValue("absent");
    private static final ConfigOption<String> COMPRESS =
            ConfigOptions.key("compress").stringType().defaultValue("absent");

    @Test
    public void testAbsent() {
        Configuration options = new Configuration();
        ParquetFileFormat parquet = new ParquetFileFormatFactory().create(options);
        assertThat(parquet.formatOptions().getString(KEY1)).isEqualTo("absent");
        assertThat(parquet.formatOptions().getString(COMPRESS)).isEqualTo("lz4");
    }

    @Test
    public void testPresent() {
        Configuration options = new Configuration();
        options.setString(KEY1.key(), "v1");
        options.setString(COMPRESS.key(), "snappy");
        ParquetFileFormat parquet = new ParquetFileFormatFactory().create(options);
        assertThat(parquet.formatOptions().getString(KEY1)).isEqualTo("v1");
        assertThat(parquet.formatOptions().getString(COMPRESS)).isEqualTo("snappy");
    }
}
