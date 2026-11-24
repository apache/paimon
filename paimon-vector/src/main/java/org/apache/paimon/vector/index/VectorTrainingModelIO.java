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

package org.apache.paimon.vector.index;

import org.apache.paimon.index.vector.CentroidRouter;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/** Reads native centroid router files with a size limit before allocating the payload. */
public final class VectorTrainingModelIO {
    private VectorTrainingModelIO() {}

    public static byte[] read(InputStream input, long fileSize) throws IOException {
        return read(input, fileSize, CentroidRouter.maxSerializedBytes());
    }

    static byte[] read(InputStream input, long fileSize, int maxPayloadBytes) throws IOException {
        if (fileSize <= 0 || fileSize > maxPayloadBytes) {
            throw new IOException("Invalid routing model payload length.");
        }
        byte[] payload = new byte[(int) fileSize];
        new DataInputStream(input).readFully(payload);
        return payload;
    }
}
