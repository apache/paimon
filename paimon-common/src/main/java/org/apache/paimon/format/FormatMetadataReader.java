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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * Interface for reading format-specific metadata to enable finer-grained splitting.
 *
 * <p>This interface allows extracting split boundaries (e.g., row groups for Parquet, stripes for
 * ORC) from files, enabling splits to be generated at finer granularity than file level.
 *
 * @since 0.9.0
 */
public interface FormatMetadataReader {

    /**
     * Get split boundaries for a file (row groups for Parquet, stripes for ORC).
     *
     * <p>Each boundary represents a portion of the file that can be read independently. The
     * boundaries are returned in order, starting from the beginning of the file.
     *
     * @param fileIO FileIO instance to read the file
     * @param filePath Path to the file
     * @param fileSize Size of the file in bytes
     * @return List of split boundaries, one per row group/stripe. Returns empty list if file cannot
     *     be split further or if an error occurs.
     * @throws IOException if an error occurs while reading file metadata
     */
    List<FileSplitBoundary> getSplitBoundaries(FileIO fileIO, Path filePath, long fileSize)
            throws IOException;

    /**
     * Check if this format supports finer-grained splitting.
     *
     * @return true if the format supports splitting files into smaller units (e.g., row groups,
     *     stripes), false otherwise
     */
    boolean supportsFinerGranularity();
}
