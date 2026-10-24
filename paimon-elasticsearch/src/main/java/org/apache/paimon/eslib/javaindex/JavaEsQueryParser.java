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

package org.apache.paimon.eslib.javaindex;

import org.apache.paimon.globalindex.EsSearchQuery;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.eslib.analyzer.BuiltinAnalyzers;
import org.elasticsearch.eslib.api.model.BuiltinAnalyzer;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.globalindex.EsSearchQuery.require;

/** Translates the validated, bounded search subset to the Java archive's Lucene queries. */
final class JavaEsQueryParser {
    private final Map<String, FieldIndexConfig> fields;

    JavaEsQueryParser(Map<String, FieldIndexConfig> fields) {
        this.fields = fields;
    }

    Query parse(EsSearchQuery query) {
        return clause(EsSearchQuery.parse(query.queryJson()));
    }

    float[] vector(JsonNode body) {
        String name = body.get("field").textValue();
        FieldIndexConfig field = field(name);
        require(
                field.indexType() == FieldIndexConfig.IndexType.VECTOR,
                "knn requires a vector field");
        JsonNode values = body.get("query_vector");
        require(
                values.size() == field.dimension(),
                "Query vector dimension does not match the index");
        float[] vector = new float[values.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = values.get(i).floatValue();
        }
        if (field.algorithm() == VectorAlgorithm.DISKBBQ
                && "cosine".equalsIgnoreCase(field.metric())) {
            VectorUtil.l2normalize(vector);
        }
        return vector;
    }

    private Query clause(JsonNode node) {
        String kind = node.fieldNames().next();
        JsonNode body = node.get(kind);
        switch (kind) {
            case "match_all":
                return new MatchAllDocsQuery();
            case "match_none":
                return new MatchNoDocsQuery();
            case "bool":
                return bool(body);
            case "exists":
                String name = body.get("field").textValue();
                field(name);
                return new FieldExistsQuery(name);
            case "knn":
                return new KnnFloatVectorQuery(
                        body.get("field").textValue(),
                        vector(body),
                        body.get("num_candidates").intValue(),
                        body.has("filter") ? clause(body.get("filter")) : null);
            default:
                String fieldName = body.fieldNames().next();
                FieldIndexConfig field = field(fieldName);
                JsonNode value = body.get(fieldName);
                switch (kind) {
                    case "term":
                        return field.indexType() == FieldIndexConfig.IndexType.FULLTEXT
                                ? new TermQuery(new Term(fieldName, value.asText()))
                                : terms(field, Collections.singletonList(value));
                    case "terms":
                        List<JsonNode> values = new ArrayList<>();
                        value.forEach(values::add);
                        return terms(field, values);
                    case "range":
                        return range(field, value);
                    case "match":
                    case "match_phrase":
                        return match(field, value, "match_phrase".equals(kind));
                    default:
                        throw new IllegalArgumentException("Unsupported Java ES query: " + kind);
                }
        }
    }

    private FieldIndexConfig field(String name) {
        FieldIndexConfig field = fields.get(name);
        require(field != null, "Field is absent from the selected ES index: " + name);
        return field;
    }

    private Query bool(JsonNode body) {
        BooleanQuery.Builder result = new BooleanQuery.Builder();
        int positive = 0;
        int required = 0;
        int should = 0;
        int total = 0;
        for (String key : Arrays.asList("must", "filter", "should", "must_not")) {
            JsonNode value = body.get(key);
            if (value == null) {
                continue;
            }
            BooleanClause.Occur occur;
            switch (key) {
                case "must":
                    occur = BooleanClause.Occur.MUST;
                    break;
                case "filter":
                    occur = BooleanClause.Occur.FILTER;
                    break;
                case "should":
                    occur = BooleanClause.Occur.SHOULD;
                    break;
                default:
                    occur = BooleanClause.Occur.MUST_NOT;
            }
            Iterable<JsonNode> children =
                    value.isArray() ? value : Collections.singletonList(value);
            for (JsonNode child : children) {
                result.add(clause(child), occur);
                total++;
                if (occur != BooleanClause.Occur.MUST_NOT) {
                    positive++;
                }
                if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER) {
                    required++;
                }
                if (occur == BooleanClause.Occur.SHOULD) {
                    should++;
                }
            }
        }
        int minimum =
                body.has("minimum_should_match")
                        ? body.get("minimum_should_match").intValue()
                        : (required == 0 && should > 0 ? 1 : 0);
        if (minimum > should) {
            return new MatchNoDocsQuery();
        }
        if (total == 0) {
            return new MatchAllDocsQuery();
        }
        if (positive == 0) {
            result.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        }
        result.setMinimumNumberShouldMatch(minimum);
        return result.build();
    }

    private Query match(FieldIndexConfig field, JsonNode value, boolean phrase) {
        require(
                field.indexType() == FieldIndexConfig.IndexType.FULLTEXT
                        || field.scalarType() == ScalarFieldType.KEYWORD,
                "match requires a fulltext or keyword field");
        BuiltinAnalyzer analyzer =
                field.scalarType() == ScalarFieldType.KEYWORD
                        ? BuiltinAnalyzer.KEYWORD
                        : field.analyzer() == null
                                ? BuiltinAnalyzer.STANDARD
                                : BuiltinAnalyzer.valueOf(field.analyzer().name());
        String text = value.isObject() ? value.get("query").textValue() : value.textValue();
        try (Analyzer instance = BuiltinAnalyzers.getAnalyzer(analyzer)) {
            QueryBuilder builder = new QueryBuilder(instance);
            Query result =
                    phrase
                            ? builder.createPhraseQuery(
                                    field.fieldName(), text, value.path("slop").asInt(0))
                            : builder.createBooleanQuery(
                                    field.fieldName(),
                                    text,
                                    "and".equals(value.path("operator").asText())
                                            ? BooleanClause.Occur.MUST
                                            : BooleanClause.Occur.SHOULD);
            if (result == null) {
                return new MatchNoDocsQuery();
            }
            return field.scalarType() == ScalarFieldType.KEYWORD
                    ? new ConstantScoreQuery(result)
                    : result;
        }
    }

    private ScalarFieldType scalar(FieldIndexConfig field) {
        if (field.indexType() == FieldIndexConfig.IndexType.FULLTEXT) {
            return ScalarFieldType.KEYWORD;
        }
        require(
                field.scalarType() != null && field.scalarType() != ScalarFieldType.GEO_POINT,
                "term/range requires a scalar or text field");
        return field.scalarType();
    }

    private Query terms(FieldIndexConfig field, List<JsonNode> values) {
        String name = field.fieldName();
        switch (scalar(field)) {
            case INT:
                int[] ints = new int[values.size()];
                for (int i = 0; i < ints.length; i++) {
                    ints[i] = decimal(values.get(i)).intValueExact();
                }
                return IntPoint.newSetQuery(name, ints);
            case LONG:
            case DATE:
                long[] longs = new long[values.size()];
                for (int i = 0; i < longs.length; i++) {
                    longs[i] = decimal(values.get(i)).longValueExact();
                }
                return LongPoint.newSetQuery(name, longs);
            case FLOAT:
                float[] floats = new float[values.size()];
                for (int i = 0; i < floats.length; i++) {
                    floats[i] = floatValue(values.get(i));
                }
                return FloatPoint.newSetQuery(name, floats);
            case DOUBLE:
                double[] doubles = new double[values.size()];
                for (int i = 0; i < doubles.length; i++) {
                    doubles[i] = doubleValue(values.get(i));
                }
                return DoublePoint.newSetQuery(name, doubles);
            default:
                List<BytesRef> terms = new ArrayList<>();
                for (JsonNode value : values) {
                    terms.add(new BytesRef(value.asText()));
                }
                return new TermInSetQuery(name, terms);
        }
    }

    private Query range(FieldIndexConfig field, JsonNode body) {
        JsonNode low = body.has("gt") ? body.get("gt") : body.get("gte");
        JsonNode high = body.has("lt") ? body.get("lt") : body.get("lte");
        boolean exclusiveLow = body.has("gt");
        boolean exclusiveHigh = body.has("lt");
        String name = field.fieldName();
        switch (scalar(field)) {
            case INT:
            case LONG:
            case DATE:
                boolean intType = field.scalarType() == ScalarFieldType.INT;
                long min = intType ? Integer.MIN_VALUE : Long.MIN_VALUE;
                long max = intType ? Integer.MAX_VALUE : Long.MAX_VALUE;
                long lower = low == null ? min : decimal(low).longValueExact();
                long upper = high == null ? max : decimal(high).longValueExact();
                if ((exclusiveLow && lower == Long.MAX_VALUE)
                        || (exclusiveHigh && upper == Long.MIN_VALUE)) {
                    return new MatchNoDocsQuery();
                }
                if (exclusiveLow) {
                    lower++;
                }
                if (exclusiveHigh) {
                    upper--;
                }
                lower = Math.max(min, lower);
                upper = Math.min(max, upper);
                if (lower > upper) {
                    return new MatchNoDocsQuery();
                }
                return intType
                        ? IntPoint.newRangeQuery(name, (int) lower, (int) upper)
                        : LongPoint.newRangeQuery(name, lower, upper);
            case FLOAT:
                float fl = low == null ? Float.NEGATIVE_INFINITY : floatValue(low);
                float fu = high == null ? Float.POSITIVE_INFINITY : floatValue(high);
                if (exclusiveLow) {
                    fl = FloatPoint.nextUp(fl);
                }
                if (exclusiveHigh) {
                    fu = FloatPoint.nextDown(fu);
                }
                return Float.compare(fl, fu) > 0
                        ? new MatchNoDocsQuery()
                        : FloatPoint.newRangeQuery(name, fl, fu);
            case DOUBLE:
                double dl = low == null ? Double.NEGATIVE_INFINITY : doubleValue(low);
                double du = high == null ? Double.POSITIVE_INFINITY : doubleValue(high);
                if (exclusiveLow) {
                    dl = DoublePoint.nextUp(dl);
                }
                if (exclusiveHigh) {
                    du = DoublePoint.nextDown(du);
                }
                return Double.compare(dl, du) > 0
                        ? new MatchNoDocsQuery()
                        : DoublePoint.newRangeQuery(name, dl, du);
            default:
                return TermRangeQuery.newStringRange(
                        name,
                        low == null ? null : low.asText(),
                        high == null ? null : high.asText(),
                        !exclusiveLow,
                        !exclusiveHigh);
        }
    }

    private static BigDecimal decimal(JsonNode value) {
        require(
                value.isNumber() || value.isTextual(),
                "Expected a numeric value (dates use epoch millis)");
        return new BigDecimal(value.asText());
    }

    private static float floatValue(JsonNode value) {
        decimal(value);
        float number = Float.parseFloat(value.asText());
        require(Float.isFinite(number), "Numeric value exceeds finite float range");
        return number;
    }

    private static double doubleValue(JsonNode value) {
        decimal(value);
        double number = Double.parseDouble(value.asText());
        require(Double.isFinite(number), "Numeric value exceeds finite double range");
        return number;
    }
}
