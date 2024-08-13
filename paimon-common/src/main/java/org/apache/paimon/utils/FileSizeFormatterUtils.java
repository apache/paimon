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

package org.apache.paimon.utils;

import java.text.DecimalFormat;

/** Supports units from bytes to terabytes. */
public class FileSizeFormatterUtils {

    private static final String[] UNITS = {"B", "KB", "MB", "GB", "TB"};
    private static final ThreadLocal<DecimalFormat> DECIMAL_FORMAT =
            ThreadLocal.withInitial(() -> new DecimalFormat("#,##0.0"));

    public static String formatFileSize(long sizeInBytes) {
        if (sizeInBytes < 0) {
            throw new IllegalArgumentException("Size in bytes cannot be negative.");
        }
        if (sizeInBytes == 0) {
            return "0B";
        }

        int unitIndex = 0;
        double sizeInUnit = sizeInBytes;

        while (sizeInUnit >= 1024 && unitIndex < UNITS.length - 1) {
            sizeInUnit /= 1024;
            unitIndex++;
        }

        return DECIMAL_FORMAT.get().format(sizeInUnit) + UNITS[unitIndex];
    }
}
