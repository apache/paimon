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

package org.apache.paimon.format;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;

/** The context for creating orc {@link RecordReader}. */
public class OrcFormatReaderContext extends FormatReaderContext {

    private final int poolSize;

    public OrcFormatReaderContext(FileIO fileIO, Path filePath, long fileSize, int poolSize) {
        super(fileIO, filePath, fileSize);
        this.poolSize = poolSize;
    }

    public int poolSize() {
        return poolSize;
    }
}
