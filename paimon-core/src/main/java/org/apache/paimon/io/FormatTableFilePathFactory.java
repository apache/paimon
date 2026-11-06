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

package org.apache.paimon.io;

import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.StringUtils.isEmpty;

/** Data-file path factory used by a Format Table writer. */
public class FormatTableFilePathFactory {

    private final DataFilePathFactory delegate;
    private final String formatIdentifier;
    private final boolean fileSuffixIncludeCompression;
    @Nullable private final String compressionExtension;
    private final boolean includeFormatSuffix;

    public FormatTableFilePathFactory(
            DataFilePathFactory delegate,
            String formatIdentifier,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            boolean includeFormatSuffix) {
        this.delegate = delegate;
        this.formatIdentifier = formatIdentifier;
        this.fileSuffixIncludeCompression = fileSuffixIncludeCompression;
        this.compressionExtension = compressionExtension(fileCompression);
        this.includeFormatSuffix = includeFormatSuffix;
    }

    public Path newPath() {
        if (includeFormatSuffix) {
            return delegate.newPath();
        }
        String extension =
                compressionExtension != null
                                && (isTextFormat(formatIdentifier) || fileSuffixIncludeCompression)
                        ? "." + compressionExtension
                        : "";
        return delegate.newPathFromExtension(extension);
    }

    @Nullable
    private static String compressionExtension(String compression) {
        if (isEmpty(compression)) {
            return null;
        }
        java.util.Optional<HadoopCompressionType> type =
                HadoopCompressionType.fromValue(compression);
        return type.isPresent() ? type.get().fileExtension() : compression;
    }

    private static boolean isTextFormat(String formatIdentifier) {
        return "json".equalsIgnoreCase(formatIdentifier)
                || "csv".equalsIgnoreCase(formatIdentifier)
                || "text".equalsIgnoreCase(formatIdentifier);
    }
}
