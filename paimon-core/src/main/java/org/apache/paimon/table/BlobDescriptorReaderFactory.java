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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.BlobDescriptorUtils;
import org.apache.paimon.utils.UriReaderFactory;

import static org.apache.paimon.CoreOptions.BLOB_DESCRIPTOR_SOURCE_TABLE;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Creates the {@link UriReaderFactory} used to resolve BLOB descriptors during writes. */
public final class BlobDescriptorReaderFactory {

    private BlobDescriptorReaderFactory() {}

    public static UriReaderFactory create(FileStoreTable table) {
        Options tableOptions = table.coreOptions().toConfiguration();
        String sourceTable = tableOptions.get(BLOB_DESCRIPTOR_SOURCE_TABLE);
        if (sourceTable != null) {
            return fromSourceTable(table, sourceTable);
        }

        CatalogContext descriptorContext =
                BlobDescriptorUtils.getCatalogContext(
                        table.catalogEnvironment().catalogContext(), tableOptions);
        return new UriReaderFactory(descriptorContext);
    }

    private static UriReaderFactory fromSourceTable(FileStoreTable table, String sourceTable) {
        CatalogLoader catalogLoader =
                checkNotNull(
                        table.catalogEnvironment().catalogLoader(),
                        "Option '%s' is not supported for tables without a catalog loader, "
                                + "including external tables in REST catalogs.",
                        BLOB_DESCRIPTOR_SOURCE_TABLE.key());
        Identifier sourceIdentifier = Identifier.fromString(sourceTable);
        try (Catalog catalog = catalogLoader.load()) {
            FileIO sourceFileIO = catalog.getTable(sourceIdentifier).fileIO();
            // Initialize lazy credentials before serializing FileIO to distributed workers.
            sourceFileIO.isObjectStore();
            return UriReaderFactory.fromFileIO(sourceFileIO);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to load BLOB descriptor source table '%s'.", sourceTable),
                    e);
        }
    }
}
