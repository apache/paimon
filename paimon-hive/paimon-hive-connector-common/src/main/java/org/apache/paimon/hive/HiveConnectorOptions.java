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

package org.apache.paimon.hive;

import org.apache.paimon.options.ConfigOption;

import static org.apache.paimon.options.ConfigOptions.key;

/** Options for hive connector. */
public class HiveConnectorOptions {

    public static final ConfigOption<Boolean> HIVE_PAIMON_RESPECT_MINMAXSPLITSIZE_ENABLED =
            key("paimon.respect.minmaxsplitsize.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, Paimon will calculate the size of split through hive parameters about splits such as 'mapreduce.input.fileinputformat.split.minsize' and 'mapreduce.input.fileinputformat.split.maxsize', and then split.");

    public static final ConfigOption<Long> HIVE_PAIMON_SPLIT_OPENFILECOST =
            key("paimon.split.openfilecost")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The cost when open a file. The config will overwrite the table property 'source.split.open-file-cost'.");
}
