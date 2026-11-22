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

package org.apache.paimon.jindo;

import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.MultiPartUploadTwoPhaseOutputStream;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/** Jindo implementation of TwoPhaseOutputStream using multipart upload. */
public class JindoTwoPhaseOutputStream
        extends MultiPartUploadTwoPhaseOutputStream<SerializableJdoObjectPart, String> {

    @Nullable private final JindoIOStats.StreamCounter stats;

    private final AtomicBoolean closed = new AtomicBoolean();

    public JindoTwoPhaseOutputStream(
            MultiPartUploadStore<SerializableJdoObjectPart, String> multiPartUploadStore,
            org.apache.hadoop.fs.Path hadoopPath,
            Path targetPath,
            @Nullable Duration ioStatsInterval)
            throws IOException {
        super(multiPartUploadStore, hadoopPath, targetPath);
        // counted once the upload has started, so a failed start leaves nothing open
        this.stats = JindoIOStats.track(JindoIOStats.OUTPUT, ioStatsInterval);
    }

    @Override
    public void write(int b) throws IOException {
        super.write(b);
        if (stats != null) {
            stats.bytes(1);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
        if (stats != null) {
            stats.bytes(len);
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        try {
            return super.closeForCommit();
        } finally {
            // even a failed attempt ends the stream: it holds no native stream and cannot retry
            if (stats != null && closed.compareAndSet(false, true)) {
                stats.closed();
            }
        }
    }

    @Override
    public Committer committer() {
        return new JindoMultiPartUploadCommitter(
                uploadId, uploadedParts, objectName, position, targetPath);
    }
}
