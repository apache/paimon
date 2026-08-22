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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.rest.HttpClientUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** An interface to read uri as a stream. */
public interface UriReader {

    SeekableInputStream newInputStream(String uri) throws IOException;

    static UriReader fromFile(FileIO fileIO) {
        return new FileUriReader(fileIO);
    }

    static UriReader fromHttp() {
        return new HttpUriReader();
    }

    static UriReader fromHttp(Duration keepAliveTimeout) {
        return new HttpUriReader(keepAliveTimeout);
    }

    /** A {@link UriReader} uses {@link FileIO} to read file. */
    class FileUriReader implements UriReader {

        private final FileIO fileIO;

        public FileUriReader(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public SeekableInputStream newInputStream(String uri) throws IOException {
            return fileIO.newInputStream(new Path(uri));
        }

        public boolean exists(String uri) throws IOException {
            return fileIO.exists(new Path(uri));
        }
    }

    /** A {@link UriReader} reads http uri. */
    class HttpUriReader implements UriReader {

        @Nullable private final Duration keepAliveTimeout;

        public HttpUriReader() {
            this.keepAliveTimeout = null;
        }

        public HttpUriReader(Duration keepAliveTimeout) {
            this.keepAliveTimeout = keepAliveTimeout;
        }

        @Override
        public SeekableInputStream newInputStream(String uri) throws IOException {
            return SeekableInputStream.wrap(
                    keepAliveTimeout == null
                            ? HttpClientUtils.getAsInputStream(uri)
                            : HttpClientUtils.getAsInputStream(uri, keepAliveTimeout));
        }

        public boolean exists(String uri) throws IOException {
            return keepAliveTimeout == null
                    ? HttpClientUtils.exists(uri)
                    : HttpClientUtils.exists(uri, keepAliveTimeout);
        }

        @Nullable
        Duration keepAliveTimeout() {
            return keepAliveTimeout;
        }
    }
}
