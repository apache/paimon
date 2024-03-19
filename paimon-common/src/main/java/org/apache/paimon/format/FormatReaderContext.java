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

/** the context for creating RecordReader {@link RecordReader}. */
public class FormatReaderContext {
    private final FileIO fileIO;
    private final Path file;
    private final Integer poolSize;
    private final Long fileSize;

    public FormatReaderContext(FileIO fileIO, Path file, Integer poolSize, Long fileSize) {
        this.fileIO = fileIO;
        this.file = file;
        this.poolSize = poolSize;
        this.fileSize = fileSize;
    }

    public FileIO getFileIO() {
        return fileIO;
    }

    public Path getFile() {
        return file;
    }

    public Integer getPoolSize() {
        return poolSize;
    }

    public Long getFileSize() {
        return fileSize;
    }
}
