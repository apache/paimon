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

package org.apache.paimon.fileindex;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** File index interface. To build a file index. */
public interface FileIndexer {

    Logger LOG = LoggerFactory.getLogger(FileIndexer.class);

    FileIndexWriter createWriter();

    /** @deprecated Override the long overload to support positions and lengths beyond int32. */
    @Deprecated
    FileIndexReader createReader(SeekableInputStream inputStream, int start, int length);

    default FileIndexReader createReader(SeekableInputStream inputStream, long start, long length) {
        if (start < Integer.MIN_VALUE
                || start > Integer.MAX_VALUE
                || length < Integer.MIN_VALUE
                || length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    String.format(
                            "File index payload range exceeds int32: start = %s, length = %s.",
                            start, length));
        }
        return createReader(inputStream, (int) start, (int) length);
    }

    static FileIndexer create(String type, DataType dataType, Options options) {
        FileIndexerFactory fileIndexerFactory = FileIndexerFactoryUtils.load(type);
        return fileIndexerFactory.create(dataType, options);
    }
}
