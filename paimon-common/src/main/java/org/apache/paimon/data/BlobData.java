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
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A {@link Blob} stores blob data directly.
 *
 * @since 1.4.0
 */
@Public
public class BlobData implements Blob, Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] data;

    public BlobData(byte[] data) {
        this.data = data;
    }

    @Override
    public byte[] toBytes() {
        return data;
    }

    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return new ByteArraySeekableStream(data);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobData blobData = (BlobData) o;
        return Objects.deepEquals(data, blobData.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
