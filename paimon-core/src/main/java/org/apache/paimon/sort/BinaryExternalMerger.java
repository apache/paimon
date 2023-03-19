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

package org.apache.paimon.sort;

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelReaderInputViewIterator;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Record merger for sort of BinaryRow. */
public class BinaryExternalMerger extends AbstractBinaryExternalMerger<BinaryRow> {

    private final BinaryRowSerializer serializer;
    private final RecordComparator comparator;

    public BinaryExternalMerger(
            IOManager ioManager,
            int pageSize,
            int maxFanIn,
            SpillChannelManager channelManager,
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        super(
                ioManager,
                pageSize,
                maxFanIn,
                channelManager,
                compressionCodecFactory,
                compressionBlockSize);
        this.serializer = serializer;
        this.comparator = comparator;
    }

    @Override
    protected MutableObjectIterator<BinaryRow> channelReaderInputViewIterator(
            ChannelReaderInputView inView) {
        return new ChannelReaderInputViewIterator(inView, null, serializer.duplicate());
    }

    @Override
    protected Comparator<BinaryRow> mergeComparator() {
        return comparator::compare;
    }

    @Override
    protected List<BinaryRow> mergeReusedEntries(int size) {
        ArrayList<BinaryRow> reused = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            reused.add(serializer.createInstance());
        }
        return reused;
    }

    @Override
    protected void writeMergingOutput(
            MutableObjectIterator<BinaryRow> mergeIterator, AbstractPagedOutputView output)
            throws IOException {
        // read the merged stream and write the data back
        BinaryRow rec = serializer.createInstance();
        while ((rec = mergeIterator.next(rec)) != null) {
            serializer.serialize(rec, output);
        }
    }
}
