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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.text.TextFileReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;

import java.io.IOException;

/** CSV file reader implementation. */
public class CsvFileReader extends TextFileReader {

    private final boolean includeHeader;
    private final CsvParser csvParser;

    private boolean headerSkipped = false;

    public CsvFileReader(
            FileIO fileIO,
            Path filePath,
            RowType rowReadType,
            RowType projectedRowType,
            CsvOptions options)
            throws IOException {
        super(fileIO, filePath, projectedRowType, options.lineDelimiter());
        this.includeHeader = options.includeHeader();
        this.csvParser =
                new CsvParser(
                        rowReadType,
                        createProjectionMapping(rowReadType, projectedRowType),
                        options);
    }

    @Override
    protected InternalRow parseLine(String line) {
        return csvParser.parse(line);
    }

    @Override
    protected void setupReading() throws IOException {
        // Skip header if needed
        if (includeHeader && !headerSkipped) {
            readLine();
            headerSkipped = true;
        }
    }

    /**
     * Creates a mapping array from read schema to projected schema. Returns indices of projected
     * columns in the read schema.
     */
    private static int[] createProjectionMapping(RowType rowReadType, RowType projectedRowType) {
        int[] mapping = new int[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            String projectedFieldName = projectedRowType.getFieldNames().get(i);
            int readIndex = rowReadType.getFieldNames().indexOf(projectedFieldName);
            if (readIndex == -1) {
                throw new IllegalArgumentException(
                        String.format(
                                "Projected field '%s' not found in read schema",
                                projectedFieldName));
            }
            mapping[i] = readIndex;
        }
        return mapping;
    }
}
