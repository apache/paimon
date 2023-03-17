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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Changelog and final result for the same key. */
public class ChangelogResult {

    private final List<KeyValue> changelogs = new ArrayList<>();
    @Nullable private KeyValue result;

    public void reset() {
        changelogs.clear();
        result = null;
    }

    public ChangelogResult addChangelog(KeyValue record) {
        changelogs.add(record);
        return this;
    }

    public ChangelogResult setResultIfNotRetract(@Nullable KeyValue result) {
        if (result != null
                && result.valueKind() != RowKind.DELETE
                && result.valueKind() != RowKind.UPDATE_BEFORE) {
            setResult(result);
        }
        return this;
    }

    public ChangelogResult setResult(@Nullable KeyValue result) {
        this.result = result;
        return this;
    }

    public List<KeyValue> changelogs() {
        return changelogs;
    }

    /** Latest result (result of merge function) for this key. */
    @Nullable
    public KeyValue result() {
        return result;
    }
}
