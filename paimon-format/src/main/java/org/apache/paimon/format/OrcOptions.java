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

package org.apache.paimon.format;

import org.apache.paimon.options.ConfigOption;

import static org.apache.paimon.options.ConfigOptions.key;

/** Options for orc format. */
public class OrcOptions {

    public static final ConfigOption<Integer> ORC_COLUMN_ENCODING_DIRECT =
            key("orc.column.encoding.direct")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of fields for which dictionary encoding is to be skipped in orc.");

    public static final ConfigOption<Double> ORC_DICTIONARY_KEY_THRESHOLD =
            key("orc.dictionary.key.threshold")
                    .doubleType()
                    .defaultValue(0.8)
                    .withDescription(
                            "If the number of distinct keys in a dictionary is greater than this "
                                    + "fraction of the total number of non-null rows, turn off "
                                    + "dictionary encoding in orc. Use 0 to always disable dictionary encoding. "
                                    + "Use 1 to always use dictionary encoding.");
}
