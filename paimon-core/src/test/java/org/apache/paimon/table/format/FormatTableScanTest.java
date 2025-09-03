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

package org.apache.paimon.table.format;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link FormatTableScan}. */
class FormatTableScanTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "File.txt",
                "file.txt",
                "123file.txt",
                "F",
                "File-1.txt",
                "a",
                "0",
                "9test",
                "Test_file.log"
            })
    @DisplayName("Test valid filenames that should return true")
    void testValidDataFileNames(String fileName) {
        assertTrue(
                FormatTableScan.isDataFileName(fileName),
                "Filename '" + fileName + "' should be valid");
    }

    @ParameterizedTest
    @ValueSource(strings = {".hidden", "_file.txt"})
    @DisplayName("Test invalid filenames that should return false")
    void testInvalidDataFileNames(String fileName) {
        assertFalse(
                FormatTableScan.isDataFileName(fileName),
                "Filename '" + fileName + "' should be invalid");
    }

    @Test
    @DisplayName("Test null input should return false")
    void testNullInput() {
        assertFalse(FormatTableScan.isDataFileName(null), "Null input should return false");
    }
}
