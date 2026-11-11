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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.util.JsonParserDelegate;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Merges JSON Object strings by replacing entire top-level property values. */
public class FieldMergeTopLevelAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private static final TypeReference<Map<String, Object>> OBJECT_TYPE =
            new TypeReference<Map<String, Object>>() {};

    public FieldMergeTopLevelAgg(String name, VarCharType fieldType) {
        super(name, fieldType);
    }

    @Override
    public boolean requireSingleInputProcessing() {
        return true;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        Map<String, Object> result = parse(accumulator);
        Map<String, Object> input = parse(inputField);
        if (result == null) {
            return inputField;
        }
        if (input == null) {
            return accumulator;
        }

        result.putAll(input);
        try {
            return BinaryString.fromString(MAPPER.writeValueAsString(result));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("merge_top_level could not serialize the result", e);
        }
    }

    private static Map<String, Object> parse(Object field) {
        if (field == null) {
            return null;
        }

        try (JsonParser parser =
                new JsonParserDelegate(MAPPER.createParser(field.toString())) {
                    @Override
                    public BigDecimal getDecimalValue() throws IOException {
                        BigDecimal number = super.getDecimalValue();
                        // Normalized decimal output must also be readable by the next merge.
                        long exponent = (long) number.precision() - number.scale() - 1;
                        checkArgument(
                                exponent <= Integer.MAX_VALUE, "Decimal exponent out of range");
                        return number;
                    }
                }) {
            Map<String, Object> result = MAPPER.readValue(parser, OBJECT_TYPE);
            checkArgument(result != null, "merge_top_level requires a JSON Object root");
            return result;
        } catch (IOException | IllegalArgumentException | ArithmeticException e) {
            throw new IllegalArgumentException(
                    "merge_top_level requires valid JSON Object strings without duplicate keys and with representable numbers",
                    e);
        }
    }
}
