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

package org.apache.paimon.fs;

import org.apache.paimon.fs.metrics.IOMetrics;

import java.io.IOException;

/** Wrap a {@link PositionOutputStream}. */
public class PositionOutputStreamIOWrapper extends PositionOutputStream {

    protected final PositionOutputStream out;
    private IOMetrics metrics;

    public PositionOutputStreamIOWrapper(PositionOutputStream out, IOMetrics metrics) {
        this.out = out;
        this.metrics = metrics;
    }

    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        try {
            out.write(b);
        } finally {
            if (metrics != null) {
                metrics.recordWriteEvent(1);
            }
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        try {
            out.write(b);
        } finally {
            if (metrics != null) {
                metrics.recordWriteEvent(b.length);
            }
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            out.write(b, off, len);
        } finally {
            if (metrics != null) {
                metrics.recordWriteEvent(len);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
