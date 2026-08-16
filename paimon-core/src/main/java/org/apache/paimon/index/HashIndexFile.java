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

package org.apache.paimon.index;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.IntIterator;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.IntFileUtils.readInts;
import static org.apache.paimon.utils.IntFileUtils.writeInts;

/** Hash index file contains ints. */
public class HashIndexFile extends IndexFile {

    public static final String HASH_INDEX = "HASH";

    public HashIndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public IntIterator read(IndexFileMeta file) throws IOException {
        return readInts(fileIO, pathFactory.toPath(file));
    }

    public List<Integer> readList(IndexFileMeta file) throws IOException {
        return IntIterator.toIntList(read(file));
    }

    public IndexFileMeta write(IntIterator input) throws IOException {
        Path path = pathFactory.newPath();
        int count = writeInts(fileIO, path, input);
        return new IndexFileMeta(
                HASH_INDEX,
                path.getName(),
                fileSize(path),
                count,
                null,
                isExternalPath() ? path.toString() : null,
                null);
    }

    public IndexFileMeta write(int[] ints) throws IOException {
        return write(IntIterator.create(ints));
    }
}
