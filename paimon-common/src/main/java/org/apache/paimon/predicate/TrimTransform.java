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
import org.apache.paimon.utils.StringUtils;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** TRIM/LTRIM/RTRIM {@link Transform}. */
public class TrimTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "TRIM";

    public static final String LTRIM = "LTRIM";

    public static final String RTRIM = "RTRIM";

    private final String trimWay;

    public TrimTransform(List<Object> inputs, String trimWay) {
        super(inputs);
        this.trimWay = trimWay;
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
        String sourceString = inputs.get(0).toString();
        String charsToTrim = inputs.size() == 1 ? " " : inputs.get(1).toString();
        switch (trimWay) {
            case NAME:
                return BinaryString.fromString(StringUtils.trim(sourceString, charsToTrim));
            case LTRIM:
                return BinaryString.fromString(StringUtils.ltrim(sourceString, charsToTrim));
            case RTRIM:
                return BinaryString.fromString(StringUtils.rtrim(sourceString, charsToTrim));
            default:
                throw new IllegalArgumentException("Invalid trim way " + trimWay);
        }
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new TrimTransform(inputs, this.trimWay);
    }
}
