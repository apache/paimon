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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.table.FormatTable;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Uses the table FileIO under the table root and the catalog-context FileIO outside it. The choice
 * comes from the registered partition path, not a listed file URI.
 */
final class FormatTableFileIOResolver {

    private final Path tableRoot;
    private final FileIO tableFileIO;
    @Nullable private final CatalogContext catalogContext;
    @Nullable private transient volatile ResolvingFileIO catalogContextFileIO;

    FormatTableFileIOResolver(FormatTable table) {
        this.tableRoot = new Path(table.location());
        this.tableFileIO = table.fileIO();
        this.catalogContext = table.catalogContext();
    }

    boolean useCatalogContextFileIO(Path partitionPath) {
        return !FormatTablePartitionPathResolver.isWithin(partitionPath, tableRoot, catalogContext);
    }

    /**
     * Resolves an external filesystem on the caller thread before parallel listing starts. The
     * underlying resolver caches the result by scheme and authority.
     */
    void prepare(Path path, boolean useCatalogContextFileIO) throws IOException {
        if (useCatalogContextFileIO) {
            catalogContextFileIO().fileIO(path);
        } else {
            tableFileIO.exists(tableRoot);
        }
    }

    FileIO fileIO(boolean useCatalogContextFileIO) {
        return useCatalogContextFileIO ? catalogContextFileIO() : tableFileIO;
    }

    private ResolvingFileIO catalogContextFileIO() {
        ResolvingFileIO result = catalogContextFileIO;
        if (result != null) {
            return result;
        }
        synchronized (this) {
            result = catalogContextFileIO;
            if (result == null) {
                if (catalogContext == null) {
                    throw new IllegalStateException(
                            "A CatalogContext is required to access a Format Table partition outside the table root.");
                }
                result = new ResolvingFileIO();
                result.configure(catalogContext);
                catalogContextFileIO = result;
            }
            return result;
        }
    }
}
