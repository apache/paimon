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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.OptionalLong;

/** A wrapper class for {@link Split} that adds query authorization information. */
public class QueryAuthSplit implements Split {

    private static final long serialVersionUID = 2L;

    private Split split;
    @Nullable private TableQueryAuthResult authResult;

    public QueryAuthSplit(Split split, @Nullable TableQueryAuthResult authResult) {
        this.split = split;
        this.authResult = authResult;
    }

    public Split split() {
        return split;
    }

    /** Unwraps one fallback layer followed by one query authorization layer. */
    public static Split unwrap(Split split) {
        if (split instanceof FallbackSplit) {
            Split wrapped = ((FallbackSplit) split).wrapped();
            // FallbackDataSplit is already a DataSplit and deliberately wraps itself.
            if (wrapped != split) {
                split = wrapped;
            }
        }
        return split instanceof QueryAuthSplit ? ((QueryAuthSplit) split).split() : split;
    }

    /** Unwraps one fallback and query authorization layer and returns the data split. */
    public static DataSplit unwrapDataSplit(Split split) {
        return (DataSplit) unwrap(split);
    }

    /** Retains the source split's query authorization result on a replacement split. */
    public static Split retainAuth(Split source, Split replacement) {
        if (source instanceof QueryAuthSplit) {
            return new QueryAuthSplit(replacement, ((QueryAuthSplit) source).authResult());
        }
        return replacement;
    }

    @Nullable
    public TableQueryAuthResult authResult() {
        return authResult;
    }

    @Override
    public long rowCount() {
        return split.rowCount();
    }

    @Override
    public OptionalLong mergedRowCount() {
        if (authResult != null) {
            List<String> filter = authResult.filter();
            if (filter != null && !filter.isEmpty()) {
                return OptionalLong.empty();
            }
        }
        return split.mergedRowCount();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        assign(deserialize(new DataInputViewStreamWrapper(in)));
    }

    private void assign(QueryAuthSplit other) {
        this.split = other.split;
        this.authResult = other.authResult;
    }

    public void serialize(DataOutputView out) throws IOException {
        SplitSerializer.serialize(this, out);
    }

    public static QueryAuthSplit deserialize(DataInputView in) throws IOException {
        Split split = SplitSerializer.deserialize(in);
        if (!(split instanceof QueryAuthSplit)) {
            throw new IOException("Deserialized split is not a QueryAuthSplit: " + split);
        }
        return (QueryAuthSplit) split;
    }
}
