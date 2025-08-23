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
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;

import java.io.IOException;

/** CSV {@link FormatReaderFactory} implementation. */
public class CsvReaderFactory implements FormatReaderFactory {

    private final RowType rowType;
    private final Options options;

    public CsvReaderFactory(RowType rowType, Options options) {
        this.rowType = rowType;
        this.options = options;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        return new CsvFileReader(context.fileIO(), context.filePath(), rowType, options);
    }
}
