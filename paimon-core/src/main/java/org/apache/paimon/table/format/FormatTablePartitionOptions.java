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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FormatTable;

import javax.annotation.Nullable;

import java.util.Locale;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Resolves the file format stored in a Format Table partition's options. */
public final class FormatTablePartitionOptions {

    private FormatTablePartitionOptions() {}

    public static String fileFormat(
            Map<String, String> tableOptions, @Nullable Map<String, String> partitionOptions) {
        String override = fileFormatOverride(partitionOptions);
        if (override != null) {
            return override;
        }
        String tableFormat = fileFormatOverride(tableOptions);
        return tableFormat == null ? CoreOptions.FILE_FORMAT.defaultValue() : tableFormat;
    }

    /**
     * Returns only an explicit override; defaults must be applied after inheriting table options.
     */
    @Nullable
    public static String fileFormatOverride(@Nullable Map<String, String> options) {
        if (options == null || !options.containsKey(CoreOptions.FILE_FORMAT.key())) {
            return null;
        }
        String value = options.get(CoreOptions.FILE_FORMAT.key());
        checkArgument(
                value != null && !value.trim().isEmpty(),
                "Format Table option '%s' must not be null or empty.",
                CoreOptions.FILE_FORMAT.key());
        return FormatTable.parseFormat(value).name().toLowerCase(Locale.ROOT);
    }
}
