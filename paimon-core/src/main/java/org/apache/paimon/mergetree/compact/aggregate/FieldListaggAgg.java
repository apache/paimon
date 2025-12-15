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

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.BinaryStringUtils.splitByWholeSeparatorPreserveAllTokens;

/** listagg aggregate a field of a row. */
public class FieldListaggAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final String delimiter;

    private final boolean distinct;

    public FieldListaggAgg(String name, VarCharType dataType, CoreOptions options, String field) {
        super(name, dataType);
        this.delimiter = options.fieldListAggDelimiter(field);
        this.distinct = options.fieldCollectAggDistinct(field);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        // ordered by type root definition

        // TODO: ensure not VARCHAR(n)
        BinaryString mergeFieldSD = (BinaryString) accumulator;
        BinaryString inFieldSD = (BinaryString) inputField;

        if (inFieldSD.getSizeInBytes() <= 0) {
            return mergeFieldSD;
        }

        if (mergeFieldSD.getSizeInBytes() <= 0) {
            return inFieldSD;
        }

        if (distinct) {
            BinaryString delimiterBinaryString = BinaryString.fromString(delimiter);

            List<BinaryString> result = new ArrayList<>();
            result.add(mergeFieldSD);
            for (BinaryString str :
                    splitByWholeSeparatorPreserveAllTokens(inFieldSD, delimiterBinaryString)) {
                if (str.getSizeInBytes() == 0 || mergeFieldSD.contains(str)) {
                    continue;
                }

                result.add(delimiterBinaryString);
                result.add(str);
            }

            if (result.size() == 1) {
                return result.get(0);
            }

            return BinaryStringUtils.concat(result);
        }

        return BinaryStringUtils.concat(
                mergeFieldSD, BinaryString.fromString(delimiter), inFieldSD);
    }
}
