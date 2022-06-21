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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;

import java.io.IOException;
import java.util.UUID;

/**
 * The AtomicFileWriter creates {@link AtomicFsDataOutputStream}.The streams do not make the files
 * they write to immediately visible, but instead write to temp files or other temporary storage. To
 * publish the data atomically in the end, the stream offers the {@link
 * RenamingAtomicFsDataOutputStream#closeAndCommit()} method to publish the result.
 */
public interface AtomicFileWriter {

    /** Opens a new atomic stream to write to the given path. */
    AtomicFsDataOutputStream open(Path path) throws IOException;

    /**
     * Write an utf8 string to file. In addition to that, it checks if the committed file is equal
     * to the input {@code content}, if not, the committing has failed.
     *
     * <p>But this does not solve overwritten committing.
     *
     * @return True if the committing was successful, False otherwise
     */
    default boolean writeFileSafety(Path path, String content) throws IOException {
        AtomicFsDataOutputStream out = open(path);
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

    static AtomicFileWriter create(FileSystem fs) throws IOException {
        RecoverableWriter recoverableWriter = null;
        try {
            recoverableWriter = fs.createRecoverableWriter();
        } catch (UnsupportedOperationException ignore) {
        }

        if (recoverableWriter == null || fs.getKind() == FileSystemKind.FILE_SYSTEM) {
            return path ->
                    new RenamingAtomicFsDataOutputStream(
                            path.getFileSystem(),
                            path,
                            new Path(path.getParent(), "." + path.getName() + UUID.randomUUID()));
        } else {
            return new RecoverableAtomicFileWriter(recoverableWriter);
        }
    }
}
