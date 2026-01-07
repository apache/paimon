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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.LongCounter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Default {@link GlobalIndexBuilder}. */
public class DefaultGlobalIndexBuilder extends GlobalIndexBuilder {
    public DefaultGlobalIndexBuilder(GlobalIndexBuilderContext context) {
        super(context);
    }

    @Override
    public List<CommitMessage> build(CloseableIterator<InternalRow> data) throws IOException {
        LongCounter rowCounter = new LongCounter(0);
        List<ResultEntry> resultEntries = writePaimonRows(data, rowCounter);
        List<IndexFileMeta> indexFileMetas =
                convertToIndexMeta(
                        context.startOffset(),
                        context.startOffset() + rowCounter.getValue() - 1,
                        resultEntries);
        DataIncrement dataIncrement = DataIncrement.indexIncrement(indexFileMetas);
        return Collections.singletonList(
                new CommitMessageImpl(
                        context.partition(),
                        0,
                        null,
                        dataIncrement,
                        CompactIncrement.emptyIncrement()));
    }

    private List<ResultEntry> writePaimonRows(
            CloseableIterator<InternalRow> rows, LongCounter rowCounter) throws IOException {
        GlobalIndexSingletonWriter indexWriter = (GlobalIndexSingletonWriter) createIndexWriter();

        InternalRow.FieldGetter getter =
                InternalRow.createFieldGetter(
                        context.indexField().type(),
                        context.readType().getFieldIndex(context.indexField().name()));
        rows.forEachRemaining(
                row -> {
                    Object indexO = getter.getFieldOrNull(row);
                    indexWriter.write(indexO);
                    rowCounter.add(1);
                });
        return indexWriter.finish();
    }
}
