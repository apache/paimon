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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

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
        SplitSerializer.serialize(this, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Split split = SplitSerializer.deserialize(new DataInputViewStreamWrapper(in));
        if (!(split instanceof QueryAuthSplit)) {
            throw new IOException("Deserialized split is not a QueryAuthSplit: " + split);
        }
        assign((QueryAuthSplit) split);
    }

    private void assign(QueryAuthSplit other) {
        this.split = other.split;
        this.authResult = other.authResult;
    }

    public void serialize(DataOutputView out) throws IOException {
        SplitSerializer.serialize(split, out);
        writeAuthResult(out, authResult);
    }

    public static QueryAuthSplit deserialize(DataInputView in) throws IOException {
        Split split = SplitSerializer.deserialize(in);
        return new QueryAuthSplit(split, readAuthResult(in));
    }

    private static void writeAuthResult(
            DataOutputView out, @Nullable TableQueryAuthResult authResult) throws IOException {
        if (authResult == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        writeStringList(out, authResult.filter());
        writeStringMap(out, authResult.columnMasking());
    }

    @Nullable
    private static TableQueryAuthResult readAuthResult(DataInputView in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        return new TableQueryAuthResult(readStringList(in), readNullableStringMap(in));
    }

    private static void writeStringList(DataOutputView out, @Nullable List<String> strings)
            throws IOException {
        if (strings == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(strings.size());
        for (String string : strings) {
            writeString(out, string);
        }
    }

    @Nullable
    private static List<String> readStringList(DataInputView in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        int size = in.readInt();
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            strings.add(readString(in));
        }
        return strings;
    }

    private static void writeStringMap(DataOutputView out, @Nullable Map<String, String> map)
            throws IOException {
        if (map == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(map.size());
        for (Map.Entry<String, String> entry : new TreeMap<>(map).entrySet()) {
            writeString(out, entry.getKey());
            writeString(out, entry.getValue());
        }
    }

    @Nullable
    private static Map<String, String> readNullableStringMap(DataInputView in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        int size = in.readInt();
        Map<String, String> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            map.put(readString(in), readString(in));
        }
        return map;
    }

    private static void writeString(DataOutputView out, @Nullable String string)
            throws IOException {
        if (string == null) {
            out.writeInt(-1);
            return;
        }

        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Nullable
    private static String readString(DataInputView in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
