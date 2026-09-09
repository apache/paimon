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

package org.apache.paimon.table.format;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.predicate.SubstringTransform;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Builds the {@code partitionNamePattern} sent to the catalog when a format table lists its
 * partitions.
 *
 * <p>Without a pattern the client drains the whole registry one page at a time, which costs one
 * round trip per page no matter how few partitions the query needs. A pattern lets the catalog
 * filter by name prefix and can reduce the number of pages returned.
 *
 * <p>{@link PartitionPathUtils#buildPartitionNamePrefixPattern} already covers the case where
 * leading partition keys are pinned by equality. This class extends the pattern by one more key
 * using predicates that constrain only a <em>prefix of that key's value</em>.
 *
 * <h2>Why a too-wide pattern is safe</h2>
 *
 * <p>The pattern only narrows the candidate set fetched from the catalog. Every candidate is still
 * checked against the full partition predicate before its files are listed (see {@code
 * CatalogSplitEnumerator}). So a pattern that matches extra partitions merely wastes a little
 * listing; a pattern that misses a matching partition would silently drop data. Every rule below is
 * therefore chosen so that <b>every partition satisfying the predicate matches the pattern</b>.
 */
final class PartitionNamePatterns {

    private PartitionNamePatterns() {}

    /**
     * Returns the pattern for a listing constrained by {@code equalityPrefix} and {@code filter},
     * or null when no useful prefix can be derived.
     */
    @Nullable
    static String build(
            List<String> partitionKeys,
            LinkedHashMap<String, String> equalityPrefix,
            @Nullable Predicate filter) {
        String base =
                PartitionPathUtils.buildPartitionNamePrefixPattern(partitionKeys, equalityPrefix);
        if (filter == null || equalityPrefix.size() >= partitionKeys.size()) {
            // Either there is nothing left to narrow, or equality already pinned every key.
            return base;
        }
        if (base == null && !equalityPrefix.isEmpty()) {
            return null;
        }

        String nextKey = partitionKeys.get(equalityPrefix.size());
        String valuePrefix = leadingValuePrefix(filter, nextKey);
        if (valuePrefix == null || valuePrefix.isEmpty()) {
            return base;
        }

        LinkedHashMap<String, String> extended = new LinkedHashMap<>(equalityPrefix);
        extended.put(nextKey, valuePrefix);
        // Escaping is per character, so escaping a value prefix yields a prefix of the escaped
        // value. generatePartitionPath appends a trailing separator that a partial value must not
        // carry.
        String path = PartitionPathUtils.generatePartitionPath(extended);
        String escaped = path.substring(0, path.length() - 1);
        if (escaped.indexOf('%') >= 0) {
            // A percent-escaped character would read as a LIKE wildcard. Widening is safe but
            // pointless, so fall back rather than send a pattern the server cannot use as an index
            // prefix.
            return base;
        }
        return escaped + '%';
    }

    /**
     * Derives a prefix of {@code key}'s value that every partition satisfying {@code filter} must
     * start with, or null if the predicates do not constrain the value's prefix.
     */
    @Nullable
    private static String leadingValuePrefix(Predicate filter, String key) {
        String substringPrefix = null;
        String startsWithPrefix = null;
        String lower = null;
        String upper = null;

        for (Predicate conjunct : PredicateBuilder.splitAnd(filter)) {
            if (!(conjunct instanceof LeafPredicate)) {
                continue;
            }
            LeafPredicate leaf = (LeafPredicate) conjunct;
            LeafFunction function = leaf.function();
            List<Object> literals = leaf.literals();
            if (literals.isEmpty() || literals.get(0) == null) {
                continue;
            }

            if (function instanceof Equal) {
                String prefix = substringPrefixOf(leaf, key);
                if (prefix != null) {
                    substringPrefix = prefix;
                }
            }

            FieldRef ref = leaf.fieldRefOptional().orElse(null);
            if (ref == null || !key.equals(ref.name()) || !isStringType(ref.type())) {
                // Prefix reasoning below compares values lexicographically. That only matches the
                // predicate's own ordering for string types: on a numeric key, `k >= 9 AND k <= 99`
                // holds for 10, whose name does not start with the common prefix "9".
                continue;
            }
            String literal = literalToString(ref.type(), literals.get(0));
            if (literal == null) {
                continue;
            }
            if (function instanceof StartsWith) {
                startsWithPrefix = literal;
            } else if (function instanceof Between) {
                // `k >= lo AND k <= hi` reaches us already folded into one Between leaf, which is
                // the shape real query plans carry; the split form below is the fallback.
                if (literals.size() == 2 && literals.get(1) != null) {
                    lower = literal;
                    upper = literalToString(ref.type(), literals.get(1));
                }
            } else if (function instanceof GreaterOrEqual || function instanceof GreaterThan) {
                lower = literal;
            } else if (function instanceof LessOrEqual || function instanceof LessThan) {
                upper = literal;
            }
        }

        if (substringPrefix != null) {
            return substringPrefix;
        }
        if (startsWithPrefix != null) {
            return startsWithPrefix;
        }
        if (lower != null && upper != null) {
            // For lexicographic order, lo <= v <= hi implies v starts with the common prefix of lo
            // and hi: if v differed from them at some position inside that prefix, it would fall
            // outside the range on that very position.
            return commonPrefix(lower, upper);
        }
        return null;
    }

    /** Returns the literal of {@code substr(key, 1, n) = 'literal'}, or null if it is not that. */
    @Nullable
    private static String substringPrefixOf(LeafPredicate leaf, String key) {
        Transform transform = leaf.transform();
        if (!(transform instanceof SubstringTransform)) {
            return null;
        }
        List<Object> inputs = transform.inputs();
        if (inputs.size() != 3 || !(inputs.get(0) instanceof FieldRef)) {
            return null;
        }
        FieldRef ref = (FieldRef) inputs.get(0);
        if (!key.equals(ref.name()) || !isStringType(ref.type())) {
            return null;
        }
        // Only a substring anchored at the first character bounds the value's prefix.
        if (!isOne(inputs.get(1))) {
            return null;
        }
        Object literal = leaf.literals().get(0);
        return literal instanceof BinaryString ? literal.toString() : null;
    }

    private static boolean isOne(Object begin) {
        if (begin instanceof Number) {
            return ((Number) begin).longValue() == 1L;
        }
        try {
            return begin != null && Long.parseLong(begin.toString()) == 1L;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isStringType(DataType type) {
        return type.getTypeRoot().getFamilies().contains(DataTypeFamily.CHARACTER_STRING);
    }

    @Nullable
    private static String literalToString(DataType type, Object literal) {
        @SuppressWarnings("unchecked")
        CastExecutor<Object, BinaryString> executor =
                (CastExecutor<Object, BinaryString>)
                        CastExecutors.resolve(type, VarCharType.STRING_TYPE);
        if (executor == null) {
            return null;
        }
        BinaryString value = executor.cast(literal);
        return value == null ? null : value.toString();
    }

    private static String commonPrefix(String a, String b) {
        int max = Math.min(a.length(), b.length());
        int i = 0;
        while (i < max) {
            int codePoint = a.codePointAt(i);
            if (codePoint != b.codePointAt(i)
                    || (Character.isSurrogate(a.charAt(i))
                            && Character.charCount(codePoint) == 1)) {
                break;
            }
            i += Character.charCount(codePoint);
        }
        return a.substring(0, i);
    }
}
