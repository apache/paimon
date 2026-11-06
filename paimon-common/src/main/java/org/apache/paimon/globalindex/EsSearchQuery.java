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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/** Bounded ES query DSL for scored searches over one grouped global index. */
public final class EsSearchQuery implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final int MAX_BYTES = 1024 * 1024;
    public static final int MAX_WINDOW = 10000;
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private final String query;
    private final int limit;
    private final Set<String> fields;

    public EsSearchQuery(String query, int limit) {
        require(limit > 0 && limit <= MAX_WINDOW, "Search window must be 1..10000");
        JsonNode node = parse(query);
        Set<String> names = new LinkedHashSet<>();
        rewrite(
                node,
                name -> {
                    names.add(name);
                    return name;
                },
                limit,
                0,
                true);
        this.query = node.toString();
        this.limit = limit;
        this.fields = Collections.unmodifiableSet(names);
    }

    public int limit() {
        return limit;
    }

    public Set<String> fields() {
        return fields;
    }

    /** Rewrites only field references, never values, using schema/index identity mappings. */
    public EsSearchQuery mapFields(Function<String, String> mapping) {
        JsonNode node = parse(query);
        rewrite(node, mapping, limit, 0, true);
        return new EsSearchQuery(node.toString(), limit);
    }

    public String queryJson() {
        return query;
    }

    /** Native archives accept top-level knn separately from the ordinary query clause. */
    public String searchSourceJson() {
        ObjectNode source = MAPPER.createObjectNode();
        JsonNode node = parse(query);
        source.set(node.has("knn") ? "knn" : "query", node.has("knn") ? node.get("knn") : node);
        source.put("from", 0);
        source.put("size", limit);
        source.put("track_total_hits", true);
        source.put("_source", false);
        return source.toString();
    }

    public static JsonNode parse(String json) {
        require(
                json != null && !json.isEmpty() && json.length() <= MAX_BYTES / 3,
                "Missing or oversized ES search JSON");
        // Check depth before constructing the tree, including rejected/unknown subtrees.
        try (JsonParser tokens = MAPPER.getFactory().createParser(json)) {
            int depth = 0;
            JsonToken token;
            while ((token = tokens.nextToken()) != null) {
                if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                    require(++depth <= 32, "ES search JSON exceeds maximum nesting depth");
                } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                    depth--;
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid ES search JSON", e);
        }
        try (JsonParser parser = MAPPER.getFactory().createParser(json)) {
            JsonNode node = MAPPER.readTree(parser);
            require(node != null && parser.nextToken() == null, "Trailing or empty ES search JSON");
            return node;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid ES search JSON", e);
        }
    }

    public static void only(JsonNode node, String... keys) {
        require(node != null && node.isObject(), "Expected a JSON object");
        Set<String> allowed = new HashSet<>(Arrays.asList(keys));
        Iterator<String> names = node.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            require(allowed.contains(name), "Unsupported ES search member: " + name);
        }
    }

    public static int integer(JsonNode node, int min, int max) {
        require(
                node != null && node.isIntegralNumber() && node.canConvertToInt(),
                "Expected an integer");
        int value = node.intValue();
        require(value >= min && value <= max, "ES search integer is out of range");
        return value;
    }

    public static String text(JsonNode node) {
        require(node != null && node.isTextual(), "Expected a string");
        String value = node.textValue();
        require(
                !value.isEmpty() && value.length() <= 1024 && value.indexOf('\0') < 0,
                "Invalid ES search name");
        return value;
    }

    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void rewrite(
            JsonNode node, Function<String, String> mapping, int limit, int depth, boolean root) {
        require(
                depth <= 16 && node.isObject() && node.size() == 1,
                "Expected exactly one bounded ES query clause");
        String kind = node.fieldNames().next();
        JsonNode body = node.get(kind);
        switch (kind) {
            case "match_all":
            case "match_none":
                only(body);
                return;
            case "bool":
                only(body, "must", "filter", "should", "must_not", "minimum_should_match");
                for (String key : Arrays.asList("must", "filter", "should", "must_not")) {
                    if (body.has(key)) {
                        JsonNode clauses = body.get(key);
                        if (clauses.isArray()) {
                            require(clauses.size() <= 256, "Too many bool clauses");
                            for (JsonNode child : clauses) {
                                rewrite(child, mapping, limit, depth + 1, false);
                            }
                        } else {
                            rewrite(clauses, mapping, limit, depth + 1, false);
                        }
                    }
                }
                if (body.has("minimum_should_match")) {
                    integer(body.get("minimum_should_match"), 0, 256);
                }
                return;
            case "exists":
                only(body, "field");
                ((ObjectNode) body).put("field", field(mapping, text(body.get("field"))));
                return;
            case "knn":
                require(root, "knn is supported only as the root query");
                only(body, "field", "query_vector", "k", "num_candidates", "filter");
                ((ObjectNode) body).put("field", field(mapping, text(body.get("field"))));
                JsonNode vector = body.get("query_vector");
                require(
                        vector != null
                                && vector.isArray()
                                && vector.size() > 0
                                && vector.size() <= 4096,
                        "Invalid query vector");
                for (JsonNode value : vector) {
                    require(
                            value.isNumber() && Float.isFinite(value.floatValue()),
                            "Vector values must be finite floats");
                }
                int k = integer(body.get("k"), limit, MAX_WINDOW);
                int candidates =
                        body.has("num_candidates")
                                ? integer(body.get("num_candidates"), k, MAX_WINDOW)
                                : k;
                ((ObjectNode) body).put("num_candidates", candidates);
                if (body.has("filter")) {
                    rewrite(body.get("filter"), mapping, limit, depth + 1, false);
                }
                return;
            case "term":
            case "terms":
            case "range":
            case "match":
            case "match_phrase":
                require(body.isObject() && body.size() == 1, "Expected exactly one query field");
                String logical = body.fieldNames().next();
                JsonNode value = body.get(logical);
                if ("range".equals(kind)) {
                    only(value, "gt", "gte", "lt", "lte");
                    require(
                            value.size() > 0
                                    && !(value.has("gt") && value.has("gte"))
                                    && !(value.has("lt") && value.has("lte")),
                            "Invalid range bounds");
                    value.elements().forEachRemaining(EsSearchQuery::literal);
                } else if ("terms".equals(kind)) {
                    require(value.isArray() && value.size() <= 10000, "Invalid terms array");
                    value.elements().forEachRemaining(EsSearchQuery::literal);
                } else if ("term".equals(kind)) {
                    literal(value);
                } else {
                    if (value.isObject()) {
                        if ("match".equals(kind)) {
                            only(value, "query", "operator");
                            if (value.has("operator")) {
                                require(
                                        Arrays.asList("and", "or")
                                                .contains(text(value.get("operator"))),
                                        "Invalid match operator");
                            }
                        } else {
                            only(value, "query", "slop");
                            if (value.has("slop")) {
                                integer(value.get("slop"), 0, 1024);
                            }
                        }
                        require(
                                value.has("query") && value.get("query").isTextual(),
                                "Match requires query text");
                    } else {
                        require(value.isTextual(), "Match requires query text");
                    }
                }
                ((ObjectNode) body).remove(logical);
                ((ObjectNode) body).set(field(mapping, logical), value);
                return;
            default:
                throw new IllegalArgumentException("Unsupported ES query clause: " + kind);
        }
    }

    private static String field(Function<String, String> mapping, String name) {
        require(
                !name.isEmpty() && name.length() <= 1024 && name.indexOf('\0') < 0,
                "Invalid query field");
        String physical = mapping.apply(name);
        require(physical != null && !physical.isEmpty(), "Unknown query field: " + name);
        return physical;
    }

    private static void literal(JsonNode node) {
        require(
                node != null && (node.isTextual() || node.isBoolean() || node.isNumber()),
                "Expected a scalar query value");
        require(
                !node.isNumber() || Double.isFinite(node.doubleValue()),
                "Query number must be finite");
    }
}
