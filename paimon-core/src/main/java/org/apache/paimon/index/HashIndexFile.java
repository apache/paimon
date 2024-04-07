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
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;

import static org.apache.paimon.utils.IntFileUtils.readInts;
import static org.apache.paimon.utils.IntFileUtils.writeInts;

/** Hash index file contains ints. */
public class HashIndexFile extends IndexFile {

    public static final String HASH_INDEX = "HASH";

    public HashIndexFile(FileIO fileIO, PathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public Path path(String fileName) {
        return pathFactory.toPath(fileName);
    }

    public IntIterator read(String fileName) throws IOException {
        return readInts(fileIO, pathFactory.toPath(fileName));
    }

    public String write(IntIterator input) throws IOException {
        Path path = pathFactory.newPath();
        writeInts(fileIO, path, input);
        return path.getName();
    }
}
