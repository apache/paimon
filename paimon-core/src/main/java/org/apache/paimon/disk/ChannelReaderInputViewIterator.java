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

package org.apache.paimon.disk;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/** A simple iterator over the input read though an I/O channel. */
public class ChannelReaderInputViewIterator implements MutableObjectIterator<BinaryRow> {
    private final ChannelReaderInputView inView;

    private final BinaryRowSerializer accessors;

    private final List<MemorySegment> freeMemTarget;

    public ChannelReaderInputViewIterator(
            ChannelReaderInputView inView,
            List<MemorySegment> freeMemTarget,
            BinaryRowSerializer accessors) {
        this.inView = inView;
        this.freeMemTarget = freeMemTarget;
        this.accessors = accessors;
    }

    @Override
    public BinaryRow next(BinaryRow reuse) throws IOException {
        try {
            return this.accessors.deserialize(reuse, this.inView);
        } catch (EOFException eofex) {
            final List<MemorySegment> freeMem = this.inView.close();
            if (this.freeMemTarget != null) {
                this.freeMemTarget.addAll(freeMem);
            }
            return null;
        }
    }

    @Override
    public BinaryRow next() throws IOException {
        try {
            return this.accessors.deserialize(this.inView);
        } catch (EOFException eofex) {
            final List<MemorySegment> freeMem = this.inView.close();
            if (this.freeMemTarget != null) {
                this.freeMemTarget.addAll(freeMem);
            }
            return null;
        }
    }
}
