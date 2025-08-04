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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for JsonFormatWriter stream handling. */
public class JsonFormatWriterStreamTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWriterCloseDoesNotThrowAlreadyClosedException() throws IOException {
        // Create a simple row type
        RowType rowType =
                RowType.of(
                        DataTypes.STRING(), // name
                        DataTypes.INT() // age
                        );

        // Create test file
        FileIO fileIO = new LocalFileIO();
        Path testPath = new Path(tempDir.toString(), "test.json");
        PositionOutputStream outputStream = fileIO.newOutputStream(testPath, false);

        // Create JSON writer
        JsonFormatWriter writer = new JsonFormatWriter(outputStream, rowType, new Options());

        // Write some data - use BinaryString for string fields
        GenericRow row1 = GenericRow.of(BinaryString.fromString("Alice"), 25);
        GenericRow row2 = GenericRow.of(BinaryString.fromString("Bob"), 30);

        writer.addElement(row1);
        writer.addElement(row2);

        // Close the writer - this should not throw "Already closed" exception
        assertDoesNotThrow(
                () -> {
                    writer.close();
                    // Try to close the output stream separately to simulate the issue
                    outputStream.flush();
                    outputStream.close();
                });
    }

    @Test
    public void testMultipleFlushAndClose() throws IOException {
        // Create a simple row type
        RowType rowType = RowType.of(DataTypes.STRING(), DataTypes.INT());

        // Create test file
        FileIO fileIO = new LocalFileIO();
        Path testPath = new Path(tempDir.toString(), "test2.json");
        PositionOutputStream outputStream = fileIO.newOutputStream(testPath, false);

        // Create JSON writer
        JsonFormatWriter writer = new JsonFormatWriter(outputStream, rowType, new Options());

        // Write and flush multiple times - use BinaryString for string fields
        GenericRow row1 = GenericRow.of(BinaryString.fromString("Alice"), 25);
        writer.addElement(row1);

        // Multiple flush/close operations should not cause issues
        assertDoesNotThrow(
                () -> {
                    writer.reachTargetSize(true, 100); // This calls flush internally
                    writer.close();
                    outputStream.close();
                });
    }
}
