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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FullTextQuery}. */
public class FullTextQueryTest {

    @Test
    public void testParseMatchQuery() {
        FullTextQuery query =
                FullTextQuery.fromJson(
                        "{\"match\":{\"terms\":\"paimon search\","
                                + "\"operator\":\"and\",\"boost\":2.0,\"fuzziness\":1}}");

        assertThat(query).isInstanceOf(FullTextQuery.Match.class);
        FullTextQuery.Match match = (FullTextQuery.Match) query;
        assertThat(match.terms()).isEqualTo("paimon search");
        assertThat(match.operator()).isEqualTo(FullTextQuery.Operator.AND);
        assertThat(match.boost()).isEqualTo(2.0f);
        assertThat(match.fuzziness()).isEqualTo(1);
        assertThat(query.toJson())
                .isEqualTo(
                        "{\"match\":{\"terms\":\"paimon search\",\"boost\":2.0,"
                                + "\"fuzziness\":1,\"max_expansions\":50,"
                                + "\"operator\":\"and\",\"prefix_length\":0}}");
    }

    @Test
    public void testParsePhraseBoostAndBooleanQuery() {
        FullTextQuery phrase =
                FullTextQuery.fromJson("{\"match_phrase\":{\"query\":\"full text\",\"slop\":1}}");
        assertThat(phrase).isInstanceOf(FullTextQuery.Phrase.class);
        assertThat(((FullTextQuery.Phrase) phrase).slop()).isEqualTo(1);
        assertThat(phrase.toJson()).isEqualTo("{\"phrase\":{\"terms\":\"full text\",\"slop\":1}}");

        FullTextQuery boost =
                FullTextQuery.fromJson(
                        "{\"boost\":{\"query\":{\"match\":{\"terms\":\"paimon\"}},"
                                + "\"factor\":3.0}}");
        assertThat(boost).isInstanceOf(FullTextQuery.Boost.class);
        assertThat(((FullTextQuery.Boost) boost).factor()).isEqualTo(3.0f);

        FullTextQuery.BooleanQuery booleanQuery =
                new FullTextQuery.BooleanQuery(
                        Collections.singletonList(FullTextQuery.match("lake")),
                        Arrays.asList(
                                FullTextQuery.match("paimon"), FullTextQuery.phrase("full text")),
                        Collections.singletonList(FullTextQuery.match("vector")));

        assertThat(booleanQuery.should()).hasSize(1);
        assertThat(booleanQuery.must()).hasSize(2);
        assertThat(booleanQuery.mustNot()).hasSize(1);
    }

    @Test
    public void testParseBooleanQueriesList() {
        FullTextQuery query =
                FullTextQuery.fromJson(
                        "{\"boolean\":{\"queries\":["
                                + "{\"occur\":\"must\",\"query\":{\"match\":{\"terms\":\"paimon\"}}},"
                                + "[\"must_not\",{\"match\":{\"terms\":\"vector\"}}]]}}");

        assertThat(query).isInstanceOf(FullTextQuery.BooleanQuery.class);
        FullTextQuery.BooleanQuery booleanQuery = (FullTextQuery.BooleanQuery) query;
        assertThat(booleanQuery.must()).hasSize(1);
        assertThat(booleanQuery.mustNot()).hasSize(1);
        assertThat(booleanQuery.should()).isEmpty();
    }

    @Test
    public void testRejectUnsupportedQueryOptions() {
        assertThatThrownBy(
                        () ->
                                FullTextQuery.fromJson(
                                        "{\"multi_match\":{\"query\":\"paimon\","
                                                + "\"columns\":[\"title\",\"body\"]}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multi_match is not supported");

        assertThatThrownBy(
                        () ->
                                FullTextQuery.fromJson(
                                        "{\"match\":{\"terms\":\"paimon\","
                                                + "\"max_expansions\":10}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxExpansions is not supported");

        assertThatThrownBy(
                        () ->
                                FullTextQuery.fromJson(
                                        "{\"match\":{\"terms\":\"paimon\","
                                                + "\"prefix_length\":1}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prefixLength is not supported");
    }

    @Test
    public void testFullTextSearchKeepsStructuredQueryJson() {
        FullTextSearch search =
                new FullTextSearch(
                        "{\"phrase\":{\"terms\":\"paimon lake\",\"slop\":1}}", 10, "content");

        assertThat(search.query()).isInstanceOf(FullTextQuery.Phrase.class);
        assertThat(search.queryText())
                .isEqualTo("{\"phrase\":{\"terms\":\"paimon lake\",\"slop\":1}}");
        assertThat(search.queryJson())
                .isEqualTo("{\"phrase\":{\"terms\":\"paimon lake\",\"slop\":1}}");
    }
}
