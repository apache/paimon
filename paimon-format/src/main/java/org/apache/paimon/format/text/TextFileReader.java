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

package org.apache.paimon.format.text;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/** TEXT format reader implementation. */
public class TextFileReader extends AbstractTextFileReader {

    private static final GenericRow emptyRow = new GenericRow(0);

    private final boolean isEmptyRow;

    public TextFileReader(
            FileIO fileIO,
            Path filePath,
            RowType projectedRowType,
            TextOptions options,
            long offset,
            @Nullable Long length)
            throws IOException {
        super(fileIO, filePath, projectedRowType, options.lineDelimiter(), offset, length);
        this.isEmptyRow = projectedRowType.getFieldCount() == 0;
    }

    @Override
    protected @Nullable InternalRow parseLine(String line) {
        if (isEmptyRow) {
            return emptyRow;
        }

        return GenericRow.of(BinaryString.fromString(line));
    }
}
