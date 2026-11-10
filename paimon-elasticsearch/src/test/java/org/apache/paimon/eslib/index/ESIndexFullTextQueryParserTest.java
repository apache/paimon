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

package org.apache.paimon.eslib.index;

import org.elasticsearch.eslib.api.model.FullTextParams;
import org.elasticsearch.eslib.api.model.FullTextQuerySpec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ESIndexFullTextQueryParserTest {

    @Test
    void acceptsPersistedColumnRoutingHints() {
        FullTextQuerySpec.Match match =
                (FullTextQuerySpec.Match)
                        ESIndexGlobalIndexReader.parseSpec(
                                "physical_text",
                                "{\"match\":{\"column\":\"logical_text\",\"terms\":\"paimon\"}}");
        assertThat(match.field()).isEqualTo("physical_text");
        assertThat(match.text()).isEqualTo("paimon");

        FullTextQuerySpec.Phrase phrase =
                (FullTextQuerySpec.Phrase)
                        ESIndexGlobalIndexReader.parseSpec(
                                "physical_text",
                                "{\"match_phrase\":{\"column\":\"logical_text\",\"terms\":\"apache paimon\",\"slop\":1}}");
        assertThat(phrase.field()).isEqualTo("physical_text");
        assertThat(phrase.text()).isEqualTo("apache paimon");
        assertThat(phrase.slop()).isEqualTo(1);

        assertThat(
                        ESIndexGlobalIndexReader.parseSpec(
                                "physical_text",
                                "{\"boolean\":{\"queries\":[[\"Must\",{\"match\":{\"column\":\"logical_text\",\"terms\":\"paimon\"}}],[\"Should\",{\"phrase\":{\"column\":\"logical_text\",\"terms\":\"apache paimon\"}}]]}}"))
                .isNotNull();
    }

    @Test
    void parsesCaseInsensitiveBooleanOperatorsAndRejectsTypos() {
        FullTextQuerySpec.Match and =
                (FullTextQuerySpec.Match)
                        ESIndexGlobalIndexReader.parseSpec(
                                "text", "{\"match\":{\"query\":\"a b\",\"operator\":\"AnD\"}}");
        assertThat(and.params().operator()).isEqualTo(FullTextParams.Operator.AND);

        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"match\":{\"query\":\"a b\",\"operator\":\"all\"}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AND or OR")
                .hasMessageContaining("all");
    }

    @Test
    void rejectsMalformedNumericAndBooleanParameters() {
        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"match\":{\"query\":\"a\",\"max_expansions\":\"many\"}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max_expansions must be an integer");

        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"bool\":{\"queries\":[[\"sometimes\",{\"match\":{\"query\":\"a\"}}]]}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MUST, SHOULD, or MUST_NOT");
    }

    @Test
    void rejectsMissingBoostChildrenAndAmbiguousQueryTypes() {
        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"boost\":{\"positive\":{\"match\":{\"query\":\"a\"}}}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");

        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"match\":{\"query\":\"a\"},\"phrase\":{\"query\":\"a\"}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void rejectsUnknownFieldsAndDuplicateAliases() {
        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"bool\":{\"mustt\":[{\"match\":{\"query\":\"a\"}}]}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown field 'mustt'")
                .hasMessageContaining("boolean query");

        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"match\":{\"query\":\"a\",\"max_expansions\":10,\"maxExpansions\":20}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both 'max_expansions' and 'maxExpansions'");

        assertThatThrownBy(
                        () ->
                                ESIndexGlobalIndexReader.parseSpec(
                                        "text",
                                        "{\"match\":{\"query\":\"a\"},\"unexpected\":true}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown field 'unexpected'");
    }

    @Test
    void rejectsExcessivelyNestedQueries() {
        String query = "{\"match\":{\"query\":\"a\"}}";
        for (int i = 0; i < 64; i++) {
            query = "{\"bool\":{\"must\":[" + query + "]}}";
        }
        String deeplyNestedQuery = query;

        assertThatThrownBy(() -> ESIndexGlobalIndexReader.parseSpec("text", deeplyNestedQuery))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum nesting depth");
    }
}
