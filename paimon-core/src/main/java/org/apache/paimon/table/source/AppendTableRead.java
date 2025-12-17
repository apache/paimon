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

package org.apache.paimon.table.source;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An abstraction layer above {@link MergeFileSplitRead} to provide reading of {@link InternalRow}.
 */
public final class AppendTableRead extends AbstractDataTableRead {

    private final List<SplitReadProvider> readProviders;

    @Nullable private RowType readType = null;
    private Predicate predicate = null;
    private TopN topN = null;
    private Integer limit = null;
    @Nullable private VariantAccessInfo[] variantAccess;

    public AppendTableRead(
            List<Function<SplitReadConfig, SplitReadProvider>> providerFactories,
            TableSchema schema) {
        super(schema);
        this.readProviders =
                providerFactories.stream()
                        .map(factory -> factory.apply(this::config))
                        .collect(Collectors.toList());
    }

    private List<SplitRead<InternalRow>> initialized() {
        List<SplitRead<InternalRow>> readers = new ArrayList<>();
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.get().initialized()) {
                readers.add(readProvider.get().get());
            }
        }
        return readers;
    }

    private void config(SplitRead<InternalRow> read) {
        if (readType != null) {
            read = read.withReadType(readType);
        }
        read.withFilter(predicate);
        read.withTopN(topN);
        read.withLimit(limit);
        read.withVariantAccess(variantAccess);
    }

    @Override
    public void applyReadType(RowType readType) {
        initialized().forEach(r -> r.withReadType(readType));
        this.readType = readType;
    }

    @Override
    public void applyVariantAccess(VariantAccessInfo[] variantAccess) {
        initialized().forEach(r -> r.withVariantAccess(variantAccess));
        this.variantAccess = variantAccess;
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        initialized().forEach(r -> r.withFilter(predicate));
        this.predicate = predicate;
        return this;
    }

    @Override
    public InnerTableRead withTopN(TopN topN) {
        initialized().forEach(r -> r.withTopN(topN));
        this.topN = topN;
        return this;
    }

    @Override
    public InnerTableRead withLimit(int limit) {
        initialized().forEach(r -> r.withLimit(limit));
        this.limit = limit;
        return this;
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.match(split, new SplitReadProvider.Context(false))) {
                return readProvider.get().get().createReader(split);
            }
        }

        throw new RuntimeException("Should not happen.");
    }
}
