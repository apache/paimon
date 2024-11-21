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

package org.apache.paimon.catalog;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Options for filesystem catalog. */
public final class FileSystemCatalogOptions {

    public static final ConfigOption<Boolean> CASE_SENSITIVE =
            ConfigOptions.key("case-sensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withFallbackKeys("allow-upper-case")
                    .withDescription(
                            "Is case sensitive. If case insensitive, you need to set this option to false, and the table name and fields be converted to lowercase.");

    private FileSystemCatalogOptions() {}
}
