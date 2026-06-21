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

package org.apache.paimon.predicate;

import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Structured query DSL for single-column full-text search. */
public abstract class FullTextQuery implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Operator for combining terms in a match query. */
    public enum Operator {
        OR,
        AND;

        static Operator fromString(String value) {
            if (value == null) {
                return OR;
            }
            String normalized = value.trim().toUpperCase(Locale.ROOT);
            if ("OR".equals(normalized)) {
                return OR;
            }
            if ("AND".equals(normalized)) {
                return AND;
            }
            throw new IllegalArgumentException(
                    "Full-text query operator must be 'or' or 'and', got: " + value);
        }

        public String jsonValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** Occurrence type for boolean full-text query clauses. */
    public enum Occur {
        SHOULD,
        MUST,
        MUST_NOT;

        static Occur fromString(String value) {
            checkNotNull(value, "Boolean query occur cannot be null");
            String normalized = value.trim().toUpperCase(Locale.ROOT);
            if ("SHOULD".equals(normalized)) {
                return SHOULD;
            }
            if ("MUST".equals(normalized)) {
                return MUST;
            }
            if ("MUST_NOT".equals(normalized) || "MUSTNOT".equals(normalized)) {
                return MUST_NOT;
            }
            throw new IllegalArgumentException("Unknown boolean query occur: " + value);
        }

        public String jsonValue() {
            return name();
        }
    }

    public static Match match(String terms) {
        return new Match(terms, 1.0f, 0, 50, Operator.OR, 0);
    }

    public static Match match(String terms, String operator) {
        return new Match(terms, 1.0f, 0, 50, Operator.fromString(operator), 0);
    }

    public static Phrase phrase(String terms) {
        return new Phrase(terms, 0);
    }

    public static Phrase phrase(String terms, int slop) {
        return new Phrase(terms, slop);
    }

    public static Boost boost(FullTextQuery query, float factor) {
        return new Boost(query, factor);
    }

    public static boolean isJsonQuery(String queryText) {
        return queryText != null && queryText.trim().startsWith("{");
    }

    public static FullTextQuery fromJson(String json) {
        JsonNode node;
        try {
            node = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse full-text query JSON: " + json, e);
        }
        return fromJsonNode(node);
    }

    private static FullTextQuery fromJsonNode(JsonNode node) {
        checkArgument(node != null && node.isObject(), "Full-text query JSON must be an object.");
        if (node.has("match")) {
            JsonNode match = node.get("match");
            return new Match(
                    textValue(match, "terms", textValue(match, "query", null)),
                    floatValue(match, "boost", 1.0f),
                    nullableIntValue(match, "fuzziness", 0),
                    intValue(match, "max_expansions", 50),
                    Operator.fromString(textValue(match, "operator", "or")),
                    intValue(match, "prefix_length", 0));
        }
        if (node.has("phrase") || node.has("match_phrase")) {
            JsonNode phrase = node.has("phrase") ? node.get("phrase") : node.get("match_phrase");
            return new Phrase(
                    textValue(phrase, "terms", textValue(phrase, "query", null)),
                    intValue(phrase, "slop", 0));
        }
        if (node.has("boost")) {
            JsonNode boost = node.get("boost");
            if (boost.has("query")) {
                return new Boost(
                        fromJsonNode(boost.get("query")),
                        floatValue(boost, "factor", floatValue(boost, "boost", 1.0f)));
            }
            if (boost.has("positive") && !boost.has("negative")) {
                return new Boost(
                        fromJsonNode(boost.get("positive")),
                        floatValue(boost, "factor", floatValue(boost, "boost", 1.0f)));
            }
            throw new IllegalArgumentException(
                    "Boost query must use {'boost': {'query': ..., 'factor': ...}}.");
        }
        if (node.has("boolean")) {
            JsonNode bool = node.get("boolean");
            List<FullTextQuery> should = queryList(bool.get("should"));
            List<FullTextQuery> must = queryList(bool.get("must"));
            List<FullTextQuery> mustNot = queryList(bool.get("must_not"));
            if (bool.has("queries")) {
                for (JsonNode clause : bool.get("queries")) {
                    Occur occur;
                    JsonNode queryNode;
                    if (clause.isArray() && clause.size() == 2) {
                        occur = Occur.fromString(clause.get(0).asText());
                        queryNode = clause.get(1);
                    } else {
                        occur = Occur.fromString(textValue(clause, "occur", null));
                        queryNode = clause.get("query");
                    }
                    addBooleanQuery(should, must, mustNot, occur, fromJsonNode(queryNode));
                }
            }
            return new BooleanQuery(should, must, mustNot);
        }
        if (node.has("multi_match")) {
            throw new IllegalArgumentException(
                    "multi_match is not supported by single-column full-text query DSL.");
        }
        throw new IllegalArgumentException("Unknown full-text query JSON: " + node);
    }

    private static List<FullTextQuery> queryList(@Nullable JsonNode node) {
        List<FullTextQuery> queries = new ArrayList<>();
        if (node == null || node.isNull()) {
            return queries;
        }
        checkArgument(node.isArray(), "Boolean query clauses must be arrays.");
        for (JsonNode child : node) {
            queries.add(fromJsonNode(child));
        }
        return queries;
    }

    private static void addBooleanQuery(
            List<FullTextQuery> should,
            List<FullTextQuery> must,
            List<FullTextQuery> mustNot,
            Occur occur,
            FullTextQuery query) {
        switch (occur) {
            case SHOULD:
                should.add(query);
                break;
            case MUST:
                must.add(query);
                break;
            case MUST_NOT:
                mustNot.add(query);
                break;
            default:
                throw new IllegalArgumentException("Unknown occur: " + occur);
        }
    }

    private static String textValue(JsonNode node, String field, @Nullable String defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return value.asText();
    }

    private static Integer nullableIntValue(
            JsonNode node, String field, @Nullable Integer defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return value.asInt();
    }

    private static int intValue(JsonNode node, String field, int defaultValue) {
        Integer value = nullableIntValue(node, field, defaultValue);
        return value == null ? defaultValue : value;
    }

    private static float floatValue(JsonNode node, String field, float defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return (float) value.asDouble();
    }

    public final String toJson() {
        return JsonSerdeUtil.toFlatJson(toRootMap());
    }

    public abstract String queryText();

    abstract Map<String, Object> toRootMap();

    private static void checkTerms(String terms) {
        checkArgument(terms != null && !terms.isEmpty(), "Query terms cannot be null or empty.");
    }

    private static void checkBoost(float boost) {
        checkArgument(boost > 0, "Boost factor must be positive, got: %s", boost);
    }

    /** Match query. */
    public static final class Match extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final String terms;
        private final float boost;
        @Nullable private final Integer fuzziness;
        private final int maxExpansions;
        private final Operator operator;
        private final int prefixLength;

        public Match(
                String terms,
                float boost,
                @Nullable Integer fuzziness,
                int maxExpansions,
                Operator operator,
                int prefixLength) {
            checkTerms(terms);
            checkBoost(boost);
            checkArgument(
                    maxExpansions > 0, "maxExpansions must be positive, got: %s", maxExpansions);
            checkArgument(
                    maxExpansions == 50,
                    "maxExpansions is not supported by Tantivy, keep the default 50.");
            checkArgument(
                    prefixLength >= 0, "prefixLength must be non-negative, got: %s", prefixLength);
            checkArgument(
                    prefixLength == 0,
                    "prefixLength is not supported by Tantivy, keep the default 0.");
            if (fuzziness != null) {
                checkArgument(fuzziness >= 0, "fuzziness must be non-negative, got: %s", fuzziness);
                checkArgument(fuzziness <= 2, "fuzziness must be <= 2, got: %s", fuzziness);
            }
            this.terms = terms;
            this.boost = boost;
            this.fuzziness = fuzziness;
            this.maxExpansions = maxExpansions;
            this.operator = operator == null ? Operator.OR : operator;
            this.prefixLength = prefixLength;
        }

        public Match withBoost(float boost) {
            return new Match(terms, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public Match withFuzziness(@Nullable Integer fuzziness) {
            return new Match(terms, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public Match withOperator(Operator operator) {
            return new Match(terms, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public String terms() {
            return terms;
        }

        public float boost() {
            return boost;
        }

        @Nullable
        public Integer fuzziness() {
            return fuzziness;
        }

        public int maxExpansions() {
            return maxExpansions;
        }

        public Operator operator() {
            return operator;
        }

        public int prefixLength() {
            return prefixLength;
        }

        @Override
        public String queryText() {
            return terms;
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("terms", terms);
            body.put("boost", boost);
            body.put("fuzziness", fuzziness);
            body.put("max_expansions", maxExpansions);
            body.put("operator", operator.jsonValue());
            body.put("prefix_length", prefixLength);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("match", body);
            return root;
        }
    }

    /** Phrase query. */
    public static final class Phrase extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final String terms;
        private final int slop;

        public Phrase(String terms, int slop) {
            checkTerms(terms);
            checkArgument(slop >= 0, "slop must be non-negative, got: %s", slop);
            this.terms = terms;
            this.slop = slop;
        }

        public String terms() {
            return terms;
        }

        public int slop() {
            return slop;
        }

        @Override
        public String queryText() {
            return terms;
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("terms", terms);
            body.put("slop", slop);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("phrase", body);
            return root;
        }
    }

    /** Score multiplier query. */
    public static final class Boost extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final FullTextQuery query;
        private final float factor;

        public Boost(FullTextQuery query, float factor) {
            this.query = checkNotNull(query, "Boost query cannot be null.");
            checkBoost(factor);
            this.factor = factor;
        }

        public FullTextQuery query() {
            return query;
        }

        public float factor() {
            return factor;
        }

        @Override
        public String queryText() {
            return query.queryText();
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("query", query.toRootMap());
            body.put("factor", factor);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("boost", body);
            return root;
        }
    }

    /** Boolean query. */
    public static final class BooleanQuery extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final List<FullTextQuery> should;
        private final List<FullTextQuery> must;
        private final List<FullTextQuery> mustNot;

        public BooleanQuery(
                List<FullTextQuery> should, List<FullTextQuery> must, List<FullTextQuery> mustNot) {
            this.should = immutableQueries(should);
            this.must = immutableQueries(must);
            this.mustNot = immutableQueries(mustNot);
            checkArgument(
                    !this.should.isEmpty() || !this.must.isEmpty() || !this.mustNot.isEmpty(),
                    "Boolean query must contain at least one clause.");
        }

        private static List<FullTextQuery> immutableQueries(List<FullTextQuery> queries) {
            List<FullTextQuery> result = new ArrayList<>();
            if (queries != null) {
                for (FullTextQuery query : queries) {
                    result.add(checkNotNull(query, "Boolean query clause cannot be null."));
                }
            }
            return Collections.unmodifiableList(result);
        }

        public BooleanQuery should(FullTextQuery query) {
            List<FullTextQuery> next = new ArrayList<>(should);
            next.add(query);
            return new BooleanQuery(next, must, mustNot);
        }

        public BooleanQuery must(FullTextQuery query) {
            List<FullTextQuery> next = new ArrayList<>(must);
            next.add(query);
            return new BooleanQuery(should, next, mustNot);
        }

        public BooleanQuery mustNot(FullTextQuery query) {
            List<FullTextQuery> next = new ArrayList<>(mustNot);
            next.add(query);
            return new BooleanQuery(should, must, next);
        }

        public List<FullTextQuery> should() {
            return should;
        }

        public List<FullTextQuery> must() {
            return must;
        }

        public List<FullTextQuery> mustNot() {
            return mustNot;
        }

        @Override
        public String queryText() {
            return "";
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("should", queryMaps(should));
            body.put("must", queryMaps(must));
            body.put("must_not", queryMaps(mustNot));
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("boolean", body);
            return root;
        }

        private static List<Map<String, Object>> queryMaps(List<FullTextQuery> queries) {
            List<Map<String, Object>> result = new ArrayList<>();
            for (FullTextQuery query : queries) {
                result.add(query.toRootMap());
            }
            return result;
        }
    }
}
