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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.format.FileFormatFactory.FormatContext;

import java.util.Optional;

/**
 * Provider for engine-specific file format implementations.
 *
 * <p>Engines can use this SPI to provide readers, writers, or validation logic without exposing the
 * implementation dependencies used by Paimon's built-in file formats. For example, an engine may
 * provide ORC or Parquet implementations backed by its own filesystem and format libraries.
 *
 * <p>The operation-specific options take precedence for their operation. If the operation-specific
 * provider is not configured or returns {@link Optional#empty()}, Paimon falls back to {@link
 * #FORMAT_PROVIDER}. If no configured provider handles the format, Paimon uses the built-in {@link
 * FileFormatFactory} for the requested format.
 */
@Experimental
public interface FileFormatProvider {

    /** Option key used to select a provider discovered from the classpath. */
    String FORMAT_PROVIDER = CoreOptions.FILE_FORMAT_PROVIDER.key();

    /** Option key used to select a provider for file-format readers. */
    String READ_FORMAT_PROVIDER = CoreOptions.FILE_FORMAT_READ_PROVIDER.key();

    /** Option key used to select a provider for file-format writers. */
    String WRITE_FORMAT_PROVIDER = CoreOptions.FILE_FORMAT_WRITE_PROVIDER.key();

    /** Option key used to select a provider for file-format validation. */
    String VALIDATION_FORMAT_PROVIDER = CoreOptions.FILE_FORMAT_VALIDATION_PROVIDER.key();

    /** Identifier used to select this provider. */
    String identifier();

    /**
     * Creates a file format for the identifier.
     *
     * <p>The identifier is the normalized file format identifier, such as {@code parquet} or {@code
     * orc}. Return {@link Optional#empty()} to let Paimon use the default service-loaded format
     * factory for this format.
     */
    Optional<FileFormat> create(String identifier, FormatContext context);
}
