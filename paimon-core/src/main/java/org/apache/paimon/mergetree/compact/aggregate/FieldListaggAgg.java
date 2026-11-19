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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.BinaryStringUtils;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.BinaryStringUtils.splitByWholeSeparatorPreserveAllTokens;

/** listagg aggregate a field of a row. */
public class FieldListaggAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final String delimiter;

    private final BinaryString delimiterBinaryString;

    private final boolean distinct;

    public FieldListaggAgg(String name, VarCharType dataType, CoreOptions options, String field) {
        super(name, dataType);
        this.delimiter = options.fieldListAggDelimiter(field);
        this.delimiterBinaryString = BinaryString.fromString(this.delimiter);
        this.distinct = options.fieldCollectAggDistinct(field);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        boolean normalizeAccumulator = distinct && delimiterBinaryString.getSizeInBytes() > 0;
        if (inputField == null || isBlank((BinaryString) inputField)) {
            if (!normalizeAccumulator
                    || accumulator == null
                    || isBlank((BinaryString) accumulator)) {
                return accumulator;
            }
            inputField = BinaryString.EMPTY_UTF8;
        }
        // ordered by type root definition

        if (accumulator == null || isBlank((BinaryString) accumulator)) {
            if (!normalizeAccumulator) {
                return inputField;
            }
            accumulator = BinaryString.EMPTY_UTF8;
        }

        BinaryString mergeFieldSD = (BinaryString) accumulator;
        BinaryString inFieldSD = (BinaryString) inputField;

        if (distinct) {
            BinaryString[] accumulatorTokens =
                    splitByWholeSeparatorPreserveAllTokens(mergeFieldSD, delimiterBinaryString);
            Set<BinaryString> existingTokens = new HashSet<>(accumulatorTokens.length);
            // Rebuilt only after finding a blank or duplicate token to drop.
            List<BinaryString> result = null;
            for (int i = 0; i < accumulatorTokens.length; i++) {
                BinaryString token = accumulatorTokens[i];
                if (!normalizeAccumulator) {
                    existingTokens.add(token);
                    continue;
                }
                boolean keep = !isBlank(token) && existingTokens.add(token);
                if (result != null) {
                    if (keep) {
                        appendToken(result, token);
                    }
                } else if (!keep) {
                    result = new ArrayList<>();
                    for (int j = 0; j < i; j++) {
                        appendToken(result, accumulatorTokens[j]);
                    }
                }
            }

            boolean changed = result != null;
            if (result == null) {
                result = new ArrayList<>();
                if (mergeFieldSD.getSizeInBytes() > 0) {
                    result.add(mergeFieldSD);
                }
            }
            for (BinaryString str :
                    splitByWholeSeparatorPreserveAllTokens(inFieldSD, delimiterBinaryString)) {
                if (isBlank(str) || !existingTokens.add(str)) {
                    continue;
                }

                appendToken(result, str);
                changed = true;
            }

            if (!changed) {
                return mergeFieldSD;
            }

            if (result.size() == 1) {
                return result.get(0);
            }

            return BinaryStringUtils.concat(result);
        }

        return BinaryStringUtils.concat(mergeFieldSD, delimiterBinaryString, inFieldSD);
    }

    private void appendToken(List<BinaryString> result, BinaryString token) {
        if (!result.isEmpty()) {
            result.add(delimiterBinaryString);
        }
        result.add(token);
    }

    /** Same as {@link StringUtils#isBlank}, but avoids decoding ASCII input to a Java string. */
    private static boolean isBlank(BinaryString str) {
        for (int i = 0; i < str.getSizeInBytes(); i++) {
            byte b = str.byteAt(i);
            if (b < 0) {
                return StringUtils.isBlank(str.toString());
            }
            if (!Character.isWhitespace(b)) {
                return false;
            }
        }
        return true;
    }
}
