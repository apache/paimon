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

package org.apache.paimon.append;

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VideoFrameDescriptor;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.SingleFileWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Rolls video pack files only between complete physical-video groups.
 *
 * <p>The target size is soft: after it is reached, immediately following frames backed by the same
 * encoded video remain in the current file. A different payload, NULL, or placeholder starts a new
 * file.
 */
class VideoRollingFileWriter<R> extends RollingFileWriterImpl<InternalRow, R> {

    private @Nullable BlobDescriptor currentVideo;
    private @Nullable BlobDescriptor nextVideo;
    private boolean pendingRoll;

    VideoRollingFileWriter(
            Supplier<? extends SingleFileWriter<InternalRow, R>> writerFactory,
            long targetFileSize,
            long targetFileRowNum) {
        super(writerFactory, targetFileSize, targetFileRowNum);
    }

    @Override
    protected void beforeWrite(InternalRow row) throws IOException {
        nextVideo = row.isNullAt(0) ? null : VideoFrameDescriptor.payloadDescriptor(row.getBlob(0));
        if (hasCurrentWriter() && pendingRoll && !Objects.equals(currentVideo, nextVideo)) {
            closeCurrentWriter();
        }
    }

    @Override
    protected void afterWrite(InternalRow row) {
        currentVideo = nextVideo;
        nextVideo = null;
    }

    @Override
    protected void onRollingCondition(InternalRow row) {
        pendingRoll = true;
    }

    @Override
    protected void onCurrentWriterClosed() {
        currentVideo = null;
        pendingRoll = false;
    }

    @Override
    public void writeBundle(BundleRecords records) throws IOException {
        for (InternalRow row : records) {
            write(row);
        }
    }
}
