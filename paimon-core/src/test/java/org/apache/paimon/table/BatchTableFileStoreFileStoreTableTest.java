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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.WriteMode;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;

/** Tests for {@link BatchFileStoreTable}. */
public class BatchTableFileStoreFileStoreTableTest extends FileStoreTableTestBase {

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Options> configure) throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.WRITE_MODE, WriteMode.TABLE);
        configure.accept(conf);
        Map<String, String> confMap = conf.toMap();
        confMap.remove(BUCKET.key());
        confMap.remove(BUCKET_KEY.key());
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                confMap,
                                ""));
        return new BatchFileStoreTable(FileIOFinder.find(tablePath), tablePath, tableSchema);
    }

    @Override
    protected FileStoreTable overwriteTestFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.WRITE_MODE, WriteMode.TABLE);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                OVERWRITE_TEST_ROW_TYPE.getFields(),
                                Arrays.asList("pt0", "pt1"),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return new BatchFileStoreTable(FileIOFinder.find(tablePath), tablePath, tableSchema);
    }

    @Test
    public void testBucketFilter() throws Exception {
        // do nothing
    }
}
