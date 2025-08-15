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

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.options.Options;

import static org.apache.paimon.format.csv.CsvFileFormat.FIELD_DELIMITER;

/** Test for {@link CsvFileFormat} use hive default field delimiter. */
public class CsvFileFormatTestWithHiveDefaultFieldDelimiter extends FormatReadWriteTest {

    protected CsvFileFormatTestWithHiveDefaultFieldDelimiter() {
        super("csv");
    }

    @Override
    protected FileFormat fileFormat() {
        Options options = new Options();
        options.set(FIELD_DELIMITER, "\001");
        return new CsvFileFormatFactory().create(new FormatContext(new Options(), 1024, 1024));
    }

    @Override
    public boolean supportNestedReadPruning() {
        return false;
    }

    @Override
    public boolean supportDataFileWithoutExtension() {
        return true;
    }
}
