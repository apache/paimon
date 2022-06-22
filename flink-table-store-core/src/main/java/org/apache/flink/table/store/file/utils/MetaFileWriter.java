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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/** File writer to write a metadata file which contains only a small amount of text data. */
public class MetaFileWriter {

    /**
     * Write an utf8 string to file. In addition to that, it checks if the committed file is equal
     * to the input {@code content}, if not, the committing has failed.
     *
     * <p>But this does not solve overwritten committing.
     *
     * @return True if the committing was successful, False otherwise
     */
    public static boolean writeFileSafety(Path path, String content) throws IOException {
        return writeFileSafety(AtomicFileWriter.create(path.getFileSystem()), path, content);
    }

    @VisibleForTesting
    static boolean writeFileSafety(AtomicFileWriter writer, Path path, String content)
            throws IOException {
        AtomicFsDataOutputStream out = writer.open(path);
        try {
            FileUtils.writeOutputStreamUtf8(out, content);
            boolean success = out.closeAndCommit();
            if (success) {
                return content.equals(FileUtils.readFileUtf8(path));
            } else {
                return false;
            }
        } catch (IOException e) {
            out.close();
            return false;
        }
    }
}
