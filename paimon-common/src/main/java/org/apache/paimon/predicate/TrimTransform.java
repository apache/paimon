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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** TRIM/LTRIM/RTRIM {@link Transform}. */
public class TrimTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "TRIM";

    /** The one-input form trims spaces only, not every whitespace character. */
    private static final BinaryString SPACE = BinaryString.fromString(" ");

    public static final String FIELD_TRIM_FLAG = "trimFlag";

    private final Flag trimFlag;

    @JsonCreator
    public TrimTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = StringTransform.InputDeserializer.class)
                    List<Object> inputs,
            @JsonProperty(FIELD_TRIM_FLAG) @JsonDeserialize(using = FlagDeserializer.class)
                    Flag trimFlag) {
        super(inputs);
        checkArgument(inputs.size() == 1 || inputs.size() == 2);
        this.trimFlag = checkNotNull(trimFlag, "trimFlag must not be null");
    }

    /** Deserializer for {@link Flag}: Jackson would also accept an ordinal or its text. */
    public static class FlagDeserializer extends JsonDeserializer<Flag> {

        @Override
        public Flag deserialize(JsonParser parser, DeserializationContext context)
                throws IOException {
            JsonNode node = parser.readValueAsTree();
            if (node.isTextual()) {
                for (Flag flag : Flag.values()) {
                    if (flag.name().equals(node.asText())) {
                        return flag;
                    }
                }
            }
            context.reportInputMismatch(
                    Flag.class, "TRIM trimFlag must be one of LEADING, TRAILING, BOTH: %s", node);
            return null;
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @JsonGetter(FIELD_TRIM_FLAG)
    public Flag trimFlag() {
        return trimFlag;
    }

    @Override
    public BinaryString transform(List<BinaryString> inputs) {
        if (inputs.get(0) == null) {
            return null;
        }
        BinaryString sourceString = inputs.get(0);
        BinaryString charsToTrim = SPACE;
        if (inputs.size() == 2) {
            if (inputs.get(1) == null) {
                return null;
            }
            charsToTrim = inputs.get(1);
        }
        switch (trimFlag) {
            case BOTH:
                return sourceString.trim(charsToTrim);
            case LEADING:
                return sourceString.trimLeft(charsToTrim);
            case TRAILING:
                return sourceString.trimRight(charsToTrim);
            default:
                throw new IllegalArgumentException("Invalid trim way " + trimFlag.name());
        }
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new TrimTransform(inputs, this.trimFlag);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        TrimTransform that = (TrimTransform) o;
        return trimFlag == that.trimFlag;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), trimFlag);
    }

    /** Enum of trim functions. */
    public enum Flag {
        LEADING,
        TRAILING,
        BOTH
    }
}
