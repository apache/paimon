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

package org.apache.paimon.format.csv;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.format.TextCompressionTest;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/** Test for CSV compression functionality. */
class CsvCompressionTest extends TextCompressionTest {

    @Override
    protected FileFormat createFileFormat(Options options) {
        return new CsvFileFormat(createFormatContext(options));
    }

    @Override
    protected String getFormatExtension() {
        return "csv";
    }

    @Test
    void testCompressionWithCustomOptions() throws IOException {
        Options options = new Options();
        options.set(CoreOptions.FILE_COMPRESSION, HadoopCompressionType.GZIP.value());
        options.set(CsvOptions.FIELD_DELIMITER, ";");
        options.set(CsvOptions.INCLUDE_HEADER, true);

        String fileName = "test_custom_options.csv.gz";
        testCompressionRoundTripWithOptions(options, fileName);
    }
}
