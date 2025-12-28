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

package org.apache.paimon.format.orc;

import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.format.FormatMetadataReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata reader for ORC files that extracts stripe boundaries for finer-grained splitting.
 *
 * <p>This reader extracts the byte offset and length for each stripe in an ORC file, enabling the
 * file to be split at stripe boundaries rather than file boundaries.
 *
 * @since 0.9.0
 */
public class OrcMetadataReader implements FormatMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(OrcMetadataReader.class);

    private final Configuration hadoopConfig;

    public OrcMetadataReader(Configuration hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    public List<FileSplitBoundary> getSplitBoundaries(FileIO fileIO, Path filePath, long fileSize)
            throws IOException {
        List<FileSplitBoundary> boundaries = new ArrayList<>();

        Reader orcReader = null;
        try {
            orcReader = OrcReaderFactory.createReader(hadoopConfig, fileIO, filePath, null);
            List<StripeInformation> stripes = orcReader.getStripes();

            if (stripes.isEmpty()) {
                LOG.warn("ORC file {} has no stripes", filePath);
                return boundaries;
            }

            for (StripeInformation stripe : stripes) {
                long offset = stripe.getOffset();
                long length = stripe.getLength();
                long rowCount = stripe.getNumberOfRows();

                boundaries.add(new FileSplitBoundary(offset, length, rowCount));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Extracted {} stripe boundaries from ORC file {}",
                        boundaries.size(),
                        filePath);
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract stripe boundaries from ORC file {}", filePath, e);
            // Return empty list on error - file will be treated as single split
            return new ArrayList<>();
        } finally {
            IOUtils.closeQuietly(orcReader);
        }

        return boundaries;
    }

    @Override
    public boolean supportsFinerGranularity() {
        return true;
    }
}
