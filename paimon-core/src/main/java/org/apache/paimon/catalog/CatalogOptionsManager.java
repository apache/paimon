/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.catalog;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.options.CatalogOptions.DATA_LINEAGE;
import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.apache.paimon.options.CatalogOptions.TABLE_LINEAGE;

/** Manage read and write of immutable {@link org.apache.paimon.options.CatalogOptions}. */
public class CatalogOptionsManager {
    private static final String OPTIONS = "options";
    public static final Set<String> IMMUTABLE_CATALOG_OPTION_KEYS =
            new HashSet<>(
                    Arrays.asList(LINEAGE_META.key(), TABLE_LINEAGE.key(), DATA_LINEAGE.key()));
    private final FileIO fileIO;
    private final Path warehouse;

    public CatalogOptionsManager(FileIO fileIO, Path warehouse) {
        this.fileIO = fileIO;
        this.warehouse = warehouse;
    }

    boolean saveImmutableOptions(Map<String, String> immutableOptions) throws IOException {
        Path catalogOptionPath = getCatalogOptionPath();
        return fileIO.writeFileUtf8(catalogOptionPath, JsonSerdeUtil.toJson(immutableOptions));
    }

    /** Read immutable catalog options. */
    Map<String, String> immutableOptions() {
        try {
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(getCatalogOptionPath()), Map.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path getCatalogOptionPath() {
        return new Path(warehouse + "/options/" + OPTIONS);
    }

    /**
     * Validate the {@link org.apache.paimon.options.CatalogOptions}.
     *
     * @param originImmutableOptions the origin persisted immutable catalog options
     * @param newImmutableOptions the new immutable catalog options
     */
    public static void validateCatalogOptions(
            Options originImmutableOptions, Options newImmutableOptions) {
        // Only open data-lineage without open table-lineage is not supported, data-lineage is
        // depend on table-lineage.
        if (newImmutableOptions.get(DATA_LINEAGE) && !newImmutableOptions.get(TABLE_LINEAGE)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Can not open %s without %s opened, please set both of them to TRUE or remove %s.",
                            CatalogOptions.DATA_LINEAGE.key(),
                            CatalogOptions.TABLE_LINEAGE.key(),
                            CatalogOptions.DATA_LINEAGE.key()));
        }

        // check immutable catalog options
        if (originImmutableOptions != null && !originImmutableOptions.equals(newImmutableOptions)) {
            throw new IllegalStateException(
                    String.format(
                            "The immutable catalog options changed, origin options are %s, new options are %s.",
                            originImmutableOptions.toMap(), newImmutableOptions.toMap()));
        }
    }
}
