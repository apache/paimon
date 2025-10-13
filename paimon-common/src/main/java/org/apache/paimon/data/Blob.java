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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.UriReader;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Blob interface, provides bytes and input stream methods.
 *
 * @since 1.4.0
 */
@Public
public interface Blob {

    byte[] toData();

    BlobDescriptor toDescriptor();

    SeekableInputStream newInputStream() throws IOException;

    static Blob fromData(byte[] data) {
        return new BlobData(data);
    }

    static Blob fromLocal(String file) {
        return fromFile(LocalFileIO.create(), file);
    }

    static Blob fromHttp(String uri) {
        return fromDescriptor(UriReader.fromHttp(), new BlobDescriptor(uri, 0, -1));
    }

    static Blob fromFile(FileIO fileIO, String file) {
        return fromFile(fileIO, file, 0, -1);
    }

    static Blob fromFile(FileIO fileIO, String file, long offset, long length) {
        return fromDescriptor(UriReader.fromFile(fileIO), new BlobDescriptor(file, offset, length));
    }

    static Blob fromDescriptor(UriReader reader, BlobDescriptor descriptor) {
        return new BlobRef(reader, descriptor);
    }

    static Blob fromInputStream(Supplier<SeekableInputStream> supplier) {
        return new BlobStream(supplier);
    }
}
