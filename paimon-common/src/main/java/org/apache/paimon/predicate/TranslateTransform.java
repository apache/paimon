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

import org.apache.paimon.data.BinaryString;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** SQL {@code TRANSLATE(input, from, to)} transform. */
public class TranslateTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "TRANSLATE";

    @JsonCreator
    public TranslateTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = StringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        super(inputs);
        checkArgument(inputs.size() == 3, "TRANSLATE requires exactly three inputs");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected BinaryString transform(List<BinaryString> inputs) {
        BinaryString source = inputs.get(0);
        BinaryString matching = inputs.get(1);
        BinaryString replacement = inputs.get(2);
        if (source == null || matching == null || replacement == null) {
            return null;
        }

        int[] matches = matching.toString().codePoints().toArray();
        int[] replacements = replacement.toString().codePoints().toArray();
        Map<Integer, Integer> dictionary = new HashMap<>();
        for (int i = 0; i < matches.length; i++) {
            // Spark keeps the first mapping for duplicate characters, including a first mapping
            // to deletion. Map.putIfAbsent cannot express that because it treats a null value as
            // absent.
            if (!dictionary.containsKey(matches[i])) {
                dictionary.put(
                        matches[i],
                        i < replacements.length && replacements[i] != 0 ? replacements[i] : null);
            }
        }

        StringBuilder result = new StringBuilder();
        source.toString()
                .codePoints()
                .forEach(
                        codePoint -> {
                            if (!dictionary.containsKey(codePoint)) {
                                result.appendCodePoint(codePoint);
                            } else if (dictionary.get(codePoint) != null) {
                                result.appendCodePoint(dictionary.get(codePoint));
                            }
                        });
        return BinaryString.fromString(result.toString());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new TranslateTransform(inputs);
    }
}
