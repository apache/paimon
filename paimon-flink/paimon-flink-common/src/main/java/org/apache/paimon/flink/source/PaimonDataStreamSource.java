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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.lineage.LineageUtils;
import org.apache.paimon.table.Table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

/**
 * A {@link Source} wrapper that preserves the wrapped source behavior and exposes Paimon lineage
 * for sources built through {@link FlinkSourceBuilder}.
 */
public class PaimonDataStreamSource<T, SplitT extends SourceSplit, CheckpointT>
        implements Source<T, SplitT, CheckpointT>, LineageVertexProvider {

    private static final long serialVersionUID = 1L;

    private final Source<T, SplitT, CheckpointT> source;
    private final Table table;

    public PaimonDataStreamSource(Source<T, SplitT, CheckpointT> source, Table table) {
        this.source = source;
        this.table = table;
    }

    @Override
    public Boundedness getBoundedness() {
        return source.getBoundedness();
    }

    @Override
    public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext)
            throws Exception {
        return source.createReader(readerContext);
    }

    @Override
    public SplitEnumerator<SplitT, CheckpointT> createEnumerator(
            SplitEnumeratorContext<SplitT> enumContext) throws Exception {
        return source.createEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<SplitT, CheckpointT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, CheckpointT checkpoint) throws Exception {
        return source.restoreEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return source.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer() {
        return source.getEnumeratorCheckpointSerializer();
    }

    @Override
    public LineageVertex getLineageVertex() {
        return LineageUtils.sourceLineageVertex(getBoundedness() == Boundedness.BOUNDED, table);
    }
}
