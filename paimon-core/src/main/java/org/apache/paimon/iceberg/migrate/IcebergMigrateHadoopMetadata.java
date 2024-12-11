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

package org.apache.paimon.iceberg.migrate;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Get iceberg table latest snapshot metadata in hadoop. */
public class IcebergMigrateHadoopMetadata implements IcebergMigrateMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMigrateHadoopMetadata.class);

    private static final String VERSION_HINT_FILENAME = "version-hint.text";
    private static final String ICEBERG_WAREHOUSE = "iceberg_warehouse";

    private final FileIO fileIO;
    private final Identifier icebergIdentifier;
    private final Options icebergOptions;

    private Path icebergLatestMetaVersionPath;
    private IcebergPathFactory icebergMetaPathFactory;

    public IcebergMigrateHadoopMetadata(
            Identifier icebergIdentifier, FileIO fileIO, Options icebergOptions) {
        this.fileIO = fileIO;
        this.icebergIdentifier = icebergIdentifier;
        this.icebergOptions = icebergOptions;
    }

    @Override
    public IcebergMetadata icebergMetadata() {
        Preconditions.checkArgument(
                icebergOptions.get(ICEBERG_WAREHOUSE) != null,
                "'iceberg_warehouse' is null. "
                        + "In hadoop-catalog, you should explicitly set this argument for finding iceberg metadata.");
        this.icebergMetaPathFactory =
                new IcebergPathFactory(
                        new Path(
                                icebergOptions.get(ICEBERG_WAREHOUSE),
                                new Path(
                                        String.format(
                                                "%s/%s/metadata",
                                                icebergIdentifier.getDatabaseName(),
                                                icebergIdentifier.getTableName()))));
        long icebergLatestMetaVersion = getIcebergLatestMetaVersion();

        this.icebergLatestMetaVersionPath =
                icebergMetaPathFactory.toMetadataPath(icebergLatestMetaVersion);
        LOG.info(
                "iceberg latest snapshot metadata file location: {}", icebergLatestMetaVersionPath);

        return IcebergMetadata.fromPath(fileIO, icebergLatestMetaVersionPath);
    }

    @Override
    public String icebergLatestMetadataLocation() {
        return icebergLatestMetaVersionPath.toString();
    }

    private long getIcebergLatestMetaVersion() {
        Path versionHintPath =
                new Path(icebergMetaPathFactory.metadataDirectory(), VERSION_HINT_FILENAME);
        try {
            return Integer.parseInt(fileIO.readFileUtf8(versionHintPath));
        } catch (IOException e) {
            throw new RuntimeException(
                    "read iceberg version-hint.text failed. Iceberg metadata path: "
                            + icebergMetaPathFactory.metadataDirectory());
        }
    }
}
