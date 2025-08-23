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

package org.apache.paimon.format.json;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.format.TextCompressionTest;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/** Test for JSON compression functionality. */
class JsonCompressionTest extends TextCompressionTest {

    @Override
    protected FileFormat createFileFormat(Options options) {
        return new JsonFileFormat(createFormatContext(options));
    }

    @Override
    protected String getFormatExtension() {
        return "json";
    }

    @Test
    void testCompressionWithCustomJsonOptions() throws IOException {
        Options options = new Options();
        options.set(CoreOptions.FILE_COMPRESSION, "gzip");
        options.set(JsonOptions.JSON_IGNORE_PARSE_ERRORS, true);
        options.set(JsonOptions.JSON_MAP_NULL_KEY_MODE, JsonOptions.MapNullKeyMode.DROP);
        options.set(JsonOptions.LINE_DELIMITER, "\n");

        String fileName = "test_custom_json_options.json.gz";
        testCompressionRoundTripWithOptions(options, fileName);
    }

    @Disabled // TODO fix dependencies
    @Test
    void testJsonCompressionWithComplexData() throws IOException {
        // Test with complex JSON structures and different compression formats
        testCompressionRoundTrip(HadoopCompressionType.GZIP.value(), "test_complex_gzip.json.gz");
        testCompressionRoundTrip(
                HadoopCompressionType.DEFLATE.value(), "test_complex_deflate.json.deflate");
        testCompressionRoundTrip(HadoopCompressionType.NONE.value(), "test_complex_none.json");
    }
}
