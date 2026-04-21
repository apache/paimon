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

package org.apache.paimon.globalindex;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.CompoundFunction;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.table.source.snapshot.TimeTravelUtil.tryTravelOrLatest;

/** A visitor to test whether a predicate is fully covered by scalar index. */
public class ScalarIndexedFieldsVisitor implements PredicateVisitor<Boolean> {

    private static final String BTREE_INDEX_TYPE = "btree";

    private final Set<String> scalarIndexedFields;

    public ScalarIndexedFieldsVisitor(Set<String> scalarIndexedFields) {
        this.scalarIndexedFields = scalarIndexedFields;
    }

    public static boolean allFieldsIndexed(Table table, @Nullable Predicate predicate) {
        if (!(table instanceof FileStoreTable)) {
            return false;
        }

        FileStoreTable storeTable = (FileStoreTable) table;

        if (predicate == null || !storeTable.coreOptions().globalIndexEnabled()) {
            return false;
        }

        Set<String> indexedFields =
                storeTable.store().newIndexFileHandler()
                        .scan(tryTravelOrLatest(storeTable), entryFilter()).stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(indexFile -> indexFile.globalIndexMeta())
                        .filter(Objects::nonNull)
                        .map(GlobalIndexMeta::indexFieldId)
                        .filter(storeTable.rowType()::containsField)
                        .map(fieldId -> storeTable.rowType().getField(fieldId).name())
                        .collect(Collectors.toSet());

        if (indexedFields.isEmpty()) {
            return false;
        }

        return predicate.visit(new ScalarIndexedFieldsVisitor(indexedFields));
    }

    private static org.apache.paimon.utils.Filter<IndexManifestEntry> entryFilter() {
        return entry -> {
            GlobalIndexMeta globalIndexMeta = entry.indexFile().globalIndexMeta();
            return globalIndexMeta != null
                    && BTREE_INDEX_TYPE.equals(entry.indexFile().indexType());
        };
    }

    @Override
    public Boolean visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return false;
        }

        FieldRef fieldRef = fieldRefOptional.get();
        if (!isScalarIndexed(fieldRef)) {
            return false;
        }

        LeafFunction function = predicate.function();
        return function instanceof Equal
                || function instanceof In
                || function instanceof Between
                || function instanceof IsNull;
    }

    @Override
    public Boolean visit(CompoundPredicate predicate) {
        CompoundFunction function = predicate.function();
        if (!(function instanceof And) && !(function instanceof Or)) {
            return false;
        }

        for (Predicate child : predicate.children()) {
            if (!child.visit(this)) {
                return false;
            }
        }
        return true;
    }

    private boolean isScalarIndexed(FieldRef fieldRef) {
        return scalarIndexedFields.contains(fieldRef.name());
    }
}
