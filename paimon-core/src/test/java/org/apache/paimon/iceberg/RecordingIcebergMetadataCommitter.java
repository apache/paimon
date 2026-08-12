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

package org.apache.paimon.iceberg;

import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.table.FileStoreTable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** An {@link IcebergMetadataCommitter} recording every commit, for tests. */
public class RecordingIcebergMetadataCommitter implements IcebergMetadataCommitter {

    public static final List<Object> COMMITS = Collections.synchronizedList(new ArrayList<>());
    public static final List<Object> BASES = Collections.synchronizedList(new ArrayList<>());
    public static volatile boolean failNextCommit = false;

    private static void maybeFail() {
        if (failNextCommit) {
            failNextCommit = false;
            throw new RuntimeException("injected catalog failure");
        }
    }

    @Override
    public String identifier() {
        return "hive";
    }

    @Override
    public void commitMetadata(Path newMetadataPath, @Nullable Path baseMetadataPath) {
        maybeFail();
        COMMITS.add(newMetadataPath);
        BASES.add(baseMetadataPath);
    }

    @Override
    public void commitMetadata(
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata) {
        maybeFail();
        COMMITS.add(newIcebergMetadata);
        BASES.add(baseIcebergMetadata);
    }

    /** Registered under hadoop-catalog: no real committer exists there, so no ambiguity. */
    public static class Factory implements IcebergMetadataCommitterFactory {

        @Override
        public String identifier() {
            return IcebergOptions.StorageType.HADOOP_CATALOG.toString();
        }

        @Override
        public IcebergMetadataCommitter create(FileStoreTable table) {
            return new RecordingIcebergMetadataCommitter();
        }
    }
}
