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

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** TRIM/LTRIM/RTRIM {@link Transform}. */
public class TrimTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "TRIM";

    /** The one-input form trims spaces only, not every whitespace character. */
    private static final BinaryString SPACE = BinaryString.fromString(" ");

    private final Flag trimFlag;

    public TrimTransform(List<Object> inputs, Flag trimFlag) {
        super(inputs);
        this.trimFlag = trimFlag;
        checkArgument(inputs.size() == 1 || inputs.size() == 2);
    }

    @Override
    public String name() {
        return NAME;
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

    /** Enum of trim functions. */
    public enum Flag {
        LEADING,
        TRAILING,
        BOTH
    }
}
