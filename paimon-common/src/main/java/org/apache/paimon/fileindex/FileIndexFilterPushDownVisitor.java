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

package org.apache.paimon.fileindex;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Visit the predicate and check if index can push down the predicate. */
public class FileIndexFilterPushDownVisitor implements PredicateVisitor<Boolean> {

    private final Map<String, List<FileIndexFilterPushDownAnalyzer>> analyzers;

    public FileIndexFilterPushDownVisitor() {
        this(Collections.emptyMap());
    }

    public FileIndexFilterPushDownVisitor(
            Map<String, List<FileIndexFilterPushDownAnalyzer>> analyzers) {
        this.analyzers = analyzers;
    }

    @Override
    public Boolean visit(LeafPredicate predicate) {
        List<FileIndexFilterPushDownAnalyzer> analyzers =
                this.analyzers.getOrDefault(predicate.fieldName(), Collections.emptyList());
        for (FileIndexFilterPushDownAnalyzer analyzer : analyzers) {
            if (analyzer.visit(predicate)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(CompoundPredicate predicate) {
        for (Predicate child : predicate.children()) {
            Boolean matched = child.visit(this);
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public Map<String, List<FileIndexFilterPushDownAnalyzer>> getAnalyzers() {
        return analyzers;
    }
}
