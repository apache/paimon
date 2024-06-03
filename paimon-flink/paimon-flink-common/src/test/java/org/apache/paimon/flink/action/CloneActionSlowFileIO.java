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

package org.apache.paimon.flink.action;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;

import java.io.IOException;

/**
 * Special {@link FileIO} for {@link CloneActionITCase}. It will sleep before reading and writing a
 * file, thus making it slower.
 */
public class CloneActionSlowFileIO extends LocalFileIO {

    public static final String SCHEME = "clone-slow";
    private static final int SLEEP_MILLIS = 150;

    @Override
    public SeekableInputStream newInputStream(Path f) throws IOException {
        return new SlowSeekableInputStreamWrapper(super.newInputStream(f));
    }

    @Override
    public PositionOutputStream newOutputStream(Path filePath, boolean overwrite)
            throws IOException {
        return new SlowPositionOutputStreamWrapper(super.newOutputStream(filePath, overwrite));
    }

    private static boolean checkStackTrace() {
        for (StackTraceElement layer : Thread.currentThread().getStackTrace()) {
            if (layer.getMethodName().contains("copyBytes")) {
                return true;
            }
        }
        return false;
    }

    private static class SlowSeekableInputStreamWrapper extends SeekableInputStreamWrapper {

        public SlowSeekableInputStreamWrapper(SeekableInputStream inputStream) {
            super(inputStream);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (checkStackTrace()) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return super.read(b, off, len);
        }
    }

    private static class SlowPositionOutputStreamWrapper extends PositionOutputStreamWrapper {

        public SlowPositionOutputStreamWrapper(PositionOutputStream outputStream) {
            super(outputStream);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (checkStackTrace()) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            super.write(b, off, len);
        }
    }

    /** Loader for {@link CloneActionSlowFileIO}. */
    public static class Loader implements FileIOLoader {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileIO load(Path path) {
            return new CloneActionSlowFileIO();
        }
    }
}
