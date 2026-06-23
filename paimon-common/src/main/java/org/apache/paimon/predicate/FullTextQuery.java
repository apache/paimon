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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Structured full-text query DSL aligned with LanceDB FTS query JSON. */
public abstract class FullTextQuery implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Operator for combining terms in a match query. */
    public enum Operator {
        OR,
        AND;

        public static Operator fromString(String value) {
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
            return name().substring(0, 1) + name().substring(1).toLowerCase(Locale.ROOT);
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
    }

    public static Match match(String query, String column) {
        return new Match(query, column, 1.0f, 0, 50, Operator.OR, 0);
    }

    public static Match match(String query, String column, String operator) {
        return match(query, column, Operator.fromString(operator));
    }

    public static Match match(String query, String column, Operator operator) {
        return new Match(query, column, 1.0f, 0, 50, operator, 0);
    }

    public static Phrase phrase(String query, String column) {
        return new Phrase(query, column, 0);
    }

    public static Phrase phrase(String query, String column, int slop) {
        return new Phrase(query, column, slop);
    }

    public static Boost boost(FullTextQuery positive, FullTextQuery negative) {
        return new Boost(positive, negative, 0.5f);
    }

    public static Boost boost(FullTextQuery positive, FullTextQuery negative, float negativeBoost) {
        return new Boost(positive, negative, negativeBoost);
    }

    public static MultiMatch multiMatch(String query, List<String> columns) {
        return new MultiMatch(query, columns, null, Operator.OR);
    }

    public static MultiMatch multiMatch(String query, List<String> columns, List<Float> boosts) {
        return new MultiMatch(query, columns, boosts, Operator.OR);
    }

    public static MultiMatch multiMatch(String query, List<String> columns, String operator) {
        return multiMatch(query, columns, null, operator);
    }

    public static MultiMatch multiMatch(
            String query, List<String> columns, List<Float> boosts, String operator) {
        return new MultiMatch(query, columns, boosts, Operator.fromString(operator));
    }

    public static MultiMatch multiMatch(
            String query, List<String> columns, List<Float> boosts, Operator operator) {
        return new MultiMatch(query, columns, boosts, operator);
    }

    public static BooleanQuery bool(List<Clause> queries) {
        return new BooleanQuery(queries);
    }

    public static BooleanQuery bool() {
        return new BooleanQuery(Collections.emptyList(), false);
    }

    public static Clause occurShould(FullTextQuery query) {
        return new Clause(Occur.SHOULD, query);
    }

    public static Clause occurMust(FullTextQuery query) {
        return new Clause(Occur.MUST, query);
    }

    public static Clause occurMustNot(FullTextQuery query) {
        return new Clause(Occur.MUST_NOT, query);
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
                    textValueAny(match, null, "terms", "query"),
                    textValue(match, "column", null),
                    floatValue(match, "boost", 1.0f),
                    fuzzinessValue(match),
                    intValueAny(match, 50, "max_expansions", "maxExpansions"),
                    Operator.fromString(textValue(match, "operator", "or")),
                    intValueAny(match, 0, "prefix_length", "prefixLength"));
        }
        if (node.has("phrase") || node.has("match_phrase")) {
            JsonNode phrase = node.has("phrase") ? node.get("phrase") : node.get("match_phrase");
            return new Phrase(
                    textValueAny(phrase, null, "terms", "query"),
                    textValue(phrase, "column", null),
                    intValue(phrase, "slop", 0));
        }
        if (node.has("boost")) {
            JsonNode boost = node.get("boost");
            return new Boost(
                    fromJsonNode(boost.get("positive")),
                    fromJsonNode(boost.get("negative")),
                    floatValueAny(boost, 0.5f, "negative_boost", "negativeBoost"));
        }
        if (node.has("multi_match")) {
            JsonNode multiMatch = node.get("multi_match");
            return new MultiMatch(
                    textValue(multiMatch, "query", null),
                    stringList(multiMatch.get("columns")),
                    nullableFloatList(fieldValue(multiMatch, "boost", "boosts")),
                    Operator.fromString(textValue(multiMatch, "operator", "or")));
        }
        if (node.has("boolean")) {
            JsonNode bool = node.get("boolean");
            List<Clause> clauses = new ArrayList<>();
            addClauses(clauses, Occur.SHOULD, bool.get("should"));
            addClauses(clauses, Occur.MUST, bool.get("must"));
            addClauses(clauses, Occur.MUST_NOT, bool.get("must_not"));
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
                    clauses.add(new Clause(occur, fromJsonNode(queryNode)));
                }
            }
            return new BooleanQuery(clauses);
        }
        throw new IllegalArgumentException("Unknown full-text query JSON: " + node);
    }

    private static void addClauses(List<Clause> clauses, Occur occur, @Nullable JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }
        checkArgument(node.isArray(), "Boolean query clauses must be arrays.");
        for (JsonNode child : node) {
            clauses.add(new Clause(occur, fromJsonNode(child)));
        }
    }

    private static String textValue(JsonNode node, String field, @Nullable String defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return value.asText();
    }

    private static String textValueAny(
            JsonNode node, @Nullable String defaultValue, String... fields) {
        JsonNode value = fieldValue(node, fields);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return value.asText();
    }

    @Nullable
    private static JsonNode fieldValue(JsonNode node, String... fields) {
        if (node == null) {
            return null;
        }
        for (String field : fields) {
            JsonNode value = node.get(field);
            if (value != null) {
                return value;
            }
        }
        return null;
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

    private static int intValueAny(JsonNode node, int defaultValue, String... fields) {
        JsonNode value = fieldValue(node, fields);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return value.asInt();
    }

    @Nullable
    private static Integer fuzzinessValue(JsonNode node) {
        JsonNode value = fieldValue(node, "fuzziness");
        if (value == null) {
            return 0;
        }
        if (value.isNull()) {
            return null;
        }
        if (value.isTextual() && "auto".equalsIgnoreCase(value.asText())) {
            return null;
        }
        return value.asInt();
    }

    private static float floatValue(JsonNode node, String field, float defaultValue) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return (float) value.asDouble();
    }

    private static float floatValueAny(JsonNode node, float defaultValue, String... fields) {
        JsonNode value = fieldValue(node, fields);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return (float) value.asDouble();
    }

    private static List<String> stringList(JsonNode node) {
        checkArgument(node != null && node.isArray(), "columns must be an array.");
        List<String> values = new ArrayList<>();
        for (JsonNode value : node) {
            values.add(value.asText());
        }
        return values;
    }

    @Nullable
    private static List<Float> nullableFloatList(@Nullable JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        checkArgument(node.isArray(), "boost must be an array.");
        List<Float> values = new ArrayList<>();
        for (JsonNode value : node) {
            values.add((float) value.asDouble());
        }
        return values;
    }

    public final String toJson() {
        return JsonSerdeUtil.toFlatJson(toRootMap());
    }

    public abstract String queryText();

    /** Returns all columns referenced by this query. */
    public abstract List<String> columns();

    public final String singleColumn() {
        List<String> columns = columns();
        checkArgument(
                columns.size() == 1,
                "Full-text query must reference exactly one column, but got: %s",
                columns);
        return columns.get(0);
    }

    abstract Map<String, Object> toRootMap();

    private static void checkColumn(String column) {
        checkArgument(column != null && !column.isEmpty(), "Column cannot be null or empty.");
    }

    private static void checkTerms(String terms) {
        checkArgument(terms != null && !terms.isEmpty(), "Query terms cannot be null or empty.");
    }

    private static void checkBoost(float boost, String name) {
        checkArgument(boost > 0, "%s must be positive, got: %s", name, boost);
    }

    private static List<String> distinctColumns(Iterable<String> columns) {
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String column : columns) {
            if (seen.add(column)) {
                result.add(column);
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** Match query. */
    public static final class Match extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final String terms;
        private final String column;
        private final float boost;
        @Nullable private final Integer fuzziness;
        private final int maxExpansions;
        private final Operator operator;
        private final int prefixLength;

        public Match(
                String terms,
                String column,
                float boost,
                @Nullable Integer fuzziness,
                int maxExpansions,
                Operator operator,
                int prefixLength) {
            checkColumn(column);
            checkTerms(terms);
            checkBoost(boost, "Boost factor");
            checkArgument(
                    maxExpansions > 0, "maxExpansions must be positive, got: %s", maxExpansions);
            checkArgument(
                    prefixLength >= 0, "prefixLength must be non-negative, got: %s", prefixLength);
            if (fuzziness != null) {
                checkArgument(fuzziness >= 0, "fuzziness must be non-negative, got: %s", fuzziness);
                checkArgument(fuzziness <= 2, "fuzziness must be <= 2, got: %s", fuzziness);
            }
            this.column = column;
            this.terms = terms;
            this.boost = boost;
            this.fuzziness = fuzziness;
            this.maxExpansions = maxExpansions;
            this.operator = operator == null ? Operator.OR : operator;
            this.prefixLength = prefixLength;
        }

        public Match withBoost(float boost) {
            return new Match(
                    terms, column, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public Match withFuzziness(@Nullable Integer fuzziness) {
            return new Match(
                    terms, column, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public Match withMaxExpansions(int maxExpansions) {
            return new Match(
                    terms, column, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public Match withOperator(Operator operator) {
            return new Match(
                    terms, column, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public Match withOperator(String operator) {
            return withOperator(Operator.fromString(operator));
        }

        public Match withPrefixLength(int prefixLength) {
            return new Match(
                    terms, column, boost, fuzziness, maxExpansions, operator, prefixLength);
        }

        public String column() {
            return column;
        }

        public String query() {
            return terms;
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
        public List<String> columns() {
            return Collections.singletonList(column);
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("column", column);
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
        private final String column;
        private final int slop;

        public Phrase(String terms, String column, int slop) {
            checkColumn(column);
            checkTerms(terms);
            checkArgument(slop >= 0, "slop must be non-negative, got: %s", slop);
            this.column = column;
            this.terms = terms;
            this.slop = slop;
        }

        public String column() {
            return column;
        }

        public String query() {
            return terms;
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
        public List<String> columns() {
            return Collections.singletonList(column);
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("column", column);
            body.put("terms", terms);
            body.put("slop", slop);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("match_phrase", body);
            return root;
        }
    }

    /** Boost query. */
    public static final class Boost extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final FullTextQuery positive;
        private final FullTextQuery negative;
        private final float negativeBoost;

        public Boost(FullTextQuery positive, FullTextQuery negative, float negativeBoost) {
            this.positive = checkNotNull(positive, "Positive boost query cannot be null.");
            this.negative = checkNotNull(negative, "Negative boost query cannot be null.");
            checkBoost(negativeBoost, "Negative boost");
            this.negativeBoost = negativeBoost;
        }

        public FullTextQuery positive() {
            return positive;
        }

        public FullTextQuery negative() {
            return negative;
        }

        public float negativeBoost() {
            return negativeBoost;
        }

        @Override
        public String queryText() {
            return positive.queryText();
        }

        @Override
        public List<String> columns() {
            List<String> columns = new ArrayList<>();
            columns.addAll(positive.columns());
            columns.addAll(negative.columns());
            return distinctColumns(columns);
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("positive", positive.toRootMap());
            body.put("negative", negative.toRootMap());
            body.put("negative_boost", negativeBoost);
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("boost", body);
            return root;
        }
    }

    /** Multi-match query. */
    public static final class MultiMatch extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final String query;
        private final List<String> columns;
        private final List<Float> boosts;
        private final Operator operator;

        public MultiMatch(
                String query,
                List<String> columns,
                @Nullable List<Float> boosts,
                Operator operator) {
            checkTerms(query);
            checkArgument(
                    columns != null && !columns.isEmpty(), "columns cannot be null or empty.");
            List<String> checkedColumns = new ArrayList<>();
            for (String column : columns) {
                checkColumn(column);
                checkedColumns.add(column);
            }
            List<Float> checkedBoosts;
            if (boosts == null) {
                checkedBoosts = new ArrayList<>(checkedColumns.size());
                for (int i = 0; i < checkedColumns.size(); i++) {
                    checkedBoosts.add(1.0f);
                }
            } else {
                checkArgument(
                        boosts.size() == checkedColumns.size(),
                        "The number of boosts must match the number of columns.");
                checkedBoosts = new ArrayList<>(boosts.size());
                for (Float boost : boosts) {
                    checkArgument(boost != null, "Boost cannot be null.");
                    checkBoost(boost, "Boost");
                    checkedBoosts.add(boost);
                }
            }
            this.query = query;
            this.columns = Collections.unmodifiableList(checkedColumns);
            this.boosts = Collections.unmodifiableList(checkedBoosts);
            this.operator = operator == null ? Operator.OR : operator;
        }

        public String query() {
            return query;
        }

        @Override
        public String queryText() {
            return query;
        }

        @Override
        public List<String> columns() {
            return columns;
        }

        public List<Float> boosts() {
            return boosts;
        }

        public Operator operator() {
            return operator;
        }

        @Override
        Map<String, Object> toRootMap() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("query", query);
            body.put("columns", columns);
            body.put("boost", boosts);
            body.put("operator", operator.jsonValue());
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("multi_match", body);
            return root;
        }
    }

    /** Boolean query clause. */
    public static final class Clause implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Occur occur;
        private final FullTextQuery query;

        public Clause(Occur occur, FullTextQuery query) {
            this.occur = checkNotNull(occur, "Boolean query occur cannot be null.");
            this.query = checkNotNull(query, "Boolean query cannot be null.");
        }

        public Occur occur() {
            return occur;
        }

        public FullTextQuery query() {
            return query;
        }
    }

    /** Boolean query. */
    public static final class BooleanQuery extends FullTextQuery {

        private static final long serialVersionUID = 1L;

        private final List<Clause> queries;
        private final List<FullTextQuery> should;
        private final List<FullTextQuery> must;
        private final List<FullTextQuery> mustNot;

        public BooleanQuery(List<Clause> queries) {
            this(queries, true);
        }

        private BooleanQuery(List<Clause> queries, boolean checkNonEmpty) {
            List<Clause> checkedQueries = new ArrayList<>();
            if (queries != null) {
                for (Clause query : queries) {
                    checkedQueries.add(checkNotNull(query, "Boolean query clause cannot be null."));
                }
            }
            if (checkNonEmpty) {
                checkArgument(
                        !checkedQueries.isEmpty(),
                        "Boolean query must contain at least one clause.");
            }
            this.queries = Collections.unmodifiableList(checkedQueries);
            this.should = filterQueries(checkedQueries, Occur.SHOULD);
            this.must = filterQueries(checkedQueries, Occur.MUST);
            this.mustNot = filterQueries(checkedQueries, Occur.MUST_NOT);
        }

        public BooleanQuery(
                List<FullTextQuery> should, List<FullTextQuery> must, List<FullTextQuery> mustNot) {
            this(toClauses(should, must, mustNot));
        }

        private static List<Clause> toClauses(
                List<FullTextQuery> should, List<FullTextQuery> must, List<FullTextQuery> mustNot) {
            List<Clause> clauses = new ArrayList<>();
            addClauses(clauses, Occur.SHOULD, should);
            addClauses(clauses, Occur.MUST, must);
            addClauses(clauses, Occur.MUST_NOT, mustNot);
            return clauses;
        }

        private static void addClauses(
                List<Clause> clauses, Occur occur, @Nullable List<FullTextQuery> queries) {
            if (queries == null) {
                return;
            }
            for (FullTextQuery query : queries) {
                clauses.add(new Clause(occur, query));
            }
        }

        private static List<FullTextQuery> filterQueries(List<Clause> queries, Occur occur) {
            List<FullTextQuery> result = new ArrayList<>();
            for (Clause query : queries) {
                if (query.occur() == occur) {
                    result.add(query.query());
                }
            }
            return Collections.unmodifiableList(result);
        }

        public BooleanQuery should(FullTextQuery query) {
            return with(Occur.SHOULD, query);
        }

        public BooleanQuery must(FullTextQuery query) {
            return with(Occur.MUST, query);
        }

        public BooleanQuery mustNot(FullTextQuery query) {
            return with(Occur.MUST_NOT, query);
        }

        private BooleanQuery with(Occur occur, FullTextQuery query) {
            List<Clause> next = new ArrayList<>(queries);
            next.add(new Clause(occur, query));
            return new BooleanQuery(next);
        }

        public List<Clause> queries() {
            return queries;
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
        public List<String> columns() {
            List<String> columns = new ArrayList<>();
            for (Clause query : queries) {
                columns.addAll(query.query().columns());
            }
            return distinctColumns(columns);
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
