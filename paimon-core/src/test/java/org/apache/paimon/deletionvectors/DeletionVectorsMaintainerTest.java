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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVectorsMaintainer}. */
public class DeletionVectorsMaintainerTest extends PrimaryKeyTableTestBase {
    private IndexFileHandler fileHandler;

    @BeforeEach
    public void beforeEach() throws Exception {
        fileHandler = table.store().newIndexFileHandler();
    }

    @Test
    public void test0() {
        DeletionVectorsMaintainer.Factory factory =
                new DeletionVectorsMaintainer.Factory(fileHandler);
        DeletionVectorsMaintainer dvMaintainer =
                factory.createOrRestore(null, BinaryRow.EMPTY_ROW, 0);

        dvMaintainer.notifyNewDeletion("f1", 1);
        dvMaintainer.notifyNewDeletion("f2", 2);
        dvMaintainer.notifyNewDeletion("f3", 3);
        dvMaintainer.removeDeletionVectorOf("f3");

        assertThat(dvMaintainer.deletionVectorOf("f1")).isPresent();
        assertThat(dvMaintainer.deletionVectorOf("f3")).isEmpty();
        List<IndexFileMeta> fileMetas = dvMaintainer.prepareCommit();

        Map<String, DeletionVector> deletionVectors = fileHandler.readAllDeletionVectors(fileMetas);
        assertThat(deletionVectors.get("f1").isDeleted(1)).isTrue();
        assertThat(deletionVectors.get("f1").isDeleted(2)).isFalse();
        assertThat(deletionVectors.get("f2").isDeleted(1)).isFalse();
        assertThat(deletionVectors.get("f2").isDeleted(2)).isTrue();
        assertThat(deletionVectors.containsKey("f3")).isFalse();
    }
}
