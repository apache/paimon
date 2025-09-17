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

package org.apache.paimon.flink.clone;

import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.utils.StringUtils;

/** Utils for file format in {@link CloneAction}. */
public class CloneFileFormatUtils {

    public static void validateFileFormat(String fileFormat) {
        if (StringUtils.isNullOrWhitespaceOnly(fileFormat)) {
            return;
        }
        String fileFormatLower = fileFormat.toLowerCase();
        String[] supportedFileFormat = new String[] {"parquet", "orc", "avro"};
        for (String supportedFormat : supportedFileFormat) {
            if (fileFormatLower.equals(supportedFormat)) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported file format: "
                        + fileFormat
                        + ". Supported file formats are: "
                        + String.join(", ", supportedFileFormat));
    }
}
