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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FullTextQuery}. */
public class FullTextQueryTest {

    @Test
    public void testMatchQueryJson() {
        FullTextQuery query =
                FullTextQuery.fromJson(
                        "{\"match\":{\"column\":\"content\",\"terms\":\"paimon search\","
                                + "\"operator\":\"And\",\"boost\":2.0,\"fuzziness\":1}}");

        assertThat(query).isInstanceOf(FullTextQuery.Match.class);
        FullTextQuery.Match match = (FullTextQuery.Match) query;
        assertThat(match.column()).isEqualTo("content");
        assertThat(match.terms()).isEqualTo("paimon search");
        assertThat(match.operator()).isEqualTo(FullTextQuery.Operator.AND);
        assertThat(match.boost()).isEqualTo(2.0f);
        assertThat(match.fuzziness()).isEqualTo(1);
        assertThat(query.toJson())
                .isEqualTo(
                        "{\"match\":{\"column\":\"content\",\"terms\":\"paimon search\","
                                + "\"boost\":2.0,\"fuzziness\":1,\"max_expansions\":50,"
                                + "\"operator\":\"And\",\"prefix_length\":0}}");
    }

    @Test
    public void testPhraseBoostMultiMatchAndBooleanJson() {
        FullTextQuery phrase =
                FullTextQuery.fromJson(
                        "{\"phrase\":{\"column\":\"content\",\"terms\":\"full text\",\"slop\":1}}");
        assertThat(phrase).isInstanceOf(FullTextQuery.Phrase.class);
        assertThat(((FullTextQuery.Phrase) phrase).column()).isEqualTo("content");
        assertThat(((FullTextQuery.Phrase) phrase).slop()).isEqualTo(1);
        assertThat(phrase.toJson())
                .isEqualTo(
                        "{\"match_phrase\":{\"column\":\"content\",\"terms\":\"full text\","
                                + "\"slop\":1}}");

        FullTextQuery boost =
                FullTextQuery.boost(
                        FullTextQuery.match("paimon", "content"),
                        FullTextQuery.match("vector", "content"),
                        0.3f);
        assertThat(boost).isInstanceOf(FullTextQuery.Boost.class);
        assertThat(((FullTextQuery.Boost) boost).negativeBoost()).isEqualTo(0.3f);
        assertThat(boost.toJson())
                .isEqualTo(
                        "{\"boost\":{\"positive\":{\"match\":{\"column\":\"content\",\"terms\":\"paimon\","
                                + "\"boost\":1.0,\"fuzziness\":0,\"max_expansions\":50,"
                                + "\"operator\":\"Or\",\"prefix_length\":0}},\"negative\":{\"match\":"
                                + "{\"column\":\"content\",\"terms\":\"vector\",\"boost\":1.0,"
                                + "\"fuzziness\":0,\"max_expansions\":50,\"operator\":\"Or\","
                                + "\"prefix_length\":0}},\"negative_boost\":0.3}}");

        FullTextQuery multiMatch =
                FullTextQuery.multiMatch("paimon", Arrays.asList("title", "content"), null, "and");
        assertThat(multiMatch).isInstanceOf(FullTextQuery.MultiMatch.class);
        assertThat(multiMatch.columns()).containsExactly("title", "content");
        assertThat(multiMatch.toJson())
                .isEqualTo(
                        "{\"multi_match\":{\"query\":\"paimon\",\"columns\":[\"title\",\"content\"],"
                                + "\"boost\":[1.0,1.0],\"operator\":\"And\"}}");

        FullTextQuery.BooleanQuery booleanQuery =
                new FullTextQuery.BooleanQuery(
                        Collections.singletonList(FullTextQuery.match("lake", "content")),
                        Arrays.asList(
                                FullTextQuery.match("paimon", "content"),
                                FullTextQuery.phrase("full text", "content")),
                        Collections.singletonList(FullTextQuery.match("vector", "content")));

        assertThat(booleanQuery.should()).hasSize(1);
        assertThat(booleanQuery.must()).hasSize(2);
        assertThat(booleanQuery.mustNot()).hasSize(1);
        assertThat(booleanQuery.singleColumn()).isEqualTo("content");
        assertThat(booleanQuery.toJson()).contains("\"boolean\"");
    }

    @Test
    public void testParseBooleanQueriesList() {
        FullTextQuery query =
                FullTextQuery.fromJson(
                        "{\"boolean\":{\"queries\":["
                                + "{\"occur\":\"must\",\"query\":{\"match\":{\"column\":\"content\","
                                + "\"terms\":\"paimon\"}}},"
                                + "[\"must_not\",{\"match\":{\"column\":\"content\","
                                + "\"terms\":\"vector\"}}]]}}");

        assertThat(query).isInstanceOf(FullTextQuery.BooleanQuery.class);
        FullTextQuery.BooleanQuery booleanQuery = (FullTextQuery.BooleanQuery) query;
        assertThat(booleanQuery.must()).hasSize(1);
        assertThat(booleanQuery.mustNot()).hasSize(1);
        assertThat(booleanQuery.should()).isEmpty();
    }

    @Test
    public void testParseLanceDbAliases() {
        FullTextQuery query =
                FullTextQuery.fromJson(
                        "{\"match\":{\"column\":\"content\",\"query\":\"paimon\","
                                + "\"maxExpansions\":10,\"prefixLength\":1,"
                                + "\"fuzziness\":\"auto\"}}");

        assertThat(query).isInstanceOf(FullTextQuery.Match.class);
        FullTextQuery.Match match = (FullTextQuery.Match) query;
        assertThat(match.terms()).isEqualTo("paimon");
        assertThat(match.maxExpansions()).isEqualTo(10);
        assertThat(match.prefixLength()).isEqualTo(1);
        assertThat(match.fuzziness()).isNull();

        FullTextQuery phrase =
                FullTextQuery.fromJson(
                        "{\"match_phrase\":{\"column\":\"content\",\"query\":\"paimon lake\"}}");
        assertThat(phrase).isInstanceOf(FullTextQuery.Phrase.class);
        assertThat(((FullTextQuery.Phrase) phrase).terms()).isEqualTo("paimon lake");

        FullTextQuery boost =
                FullTextQuery.fromJson(
                        "{\"boost\":{\"positive\":{\"match\":{\"column\":\"content\","
                                + "\"terms\":\"paimon\"}},\"negative\":{\"match\":"
                                + "{\"column\":\"content\",\"terms\":\"vector\"}},"
                                + "\"negativeBoost\":0.2}}");
        assertThat(((FullTextQuery.Boost) boost).negativeBoost()).isEqualTo(0.2f);

        FullTextQuery multiMatch =
                FullTextQuery.fromJson(
                        "{\"multi_match\":{\"query\":\"paimon\","
                                + "\"columns\":[\"title\",\"content\"],"
                                + "\"boosts\":[2.0,1.0]}}");
        assertThat(((FullTextQuery.MultiMatch) multiMatch).boosts()).containsExactly(2.0f, 1.0f);
    }

    @Test
    public void testFullTextSearchKeepsStructuredQueryJson() {
        FullTextSearch search =
                new FullTextSearch(
                        FullTextQuery.fromJson(
                                "{\"phrase\":{\"column\":\"content\","
                                        + "\"terms\":\"paimon lake\",\"slop\":1}}"),
                        10);

        assertThat(search.fieldName()).isEqualTo("content");
        assertThat(search.query()).isInstanceOf(FullTextQuery.Phrase.class);
        assertThat(search.queryJson())
                .isEqualTo(
                        "{\"match_phrase\":{\"column\":\"content\",\"terms\":\"paimon lake\","
                                + "\"slop\":1}}");
    }
}
