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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * A stream initially writes to hidden files or temp files and only creates the target file once it
 * is closed and "committed".
 */
public class RenamingAtomicFsDataOutputStream extends AtomicFsDataOutputStream {

    private final FileSystem fs;
    private final Path targetFile;
    private final Path tempFile;
    private final FSDataOutputStream out;

    public RenamingAtomicFsDataOutputStream(FileSystem fs, Path targetFile, Path tempFile)
            throws IOException {
        this.fs = fs;
        this.targetFile = targetFile;
        this.tempFile = tempFile;
        this.out = fs.create(tempFile, FileSystem.WriteMode.OVERWRITE);
    }

    @Override
    public void write(int b) throws IOException {
        this.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        this.out.flush();
    }

    @Override
    public void sync() throws IOException {
        this.out.sync();
    }

    @Override
    public long getPos() throws IOException {
        return this.out.getPos();
    }

    @Override
    public boolean closeAndCommit() throws IOException {
        final long offset = getPos();
        out.close();

        final FileStatus srcStatus;
        try {
            srcStatus = fs.getFileStatus(tempFile);
        } catch (IOException e) {
            throw new IOException("Cannot clean commit: Staging file does not exist.");
        }

        if (srcStatus.getLen() != offset) {
            throw new IOException("Cannot clean commit: File has trailing junk data.");
        }

        try {
            boolean success = fs.rename(tempFile, targetFile);
            if (success) {
                return true;
            } else {
                FileUtils.deleteOrWarn(tempFile);
                return false;
            }
        } catch (IOException e) {
            FileUtils.deleteOrWarn(tempFile);
            throw new IOException(
                    "Committing file by rename failed: " + tempFile + " to " + targetFile, e);
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
        FileUtils.deleteOrWarn(tempFile);
    }
}
