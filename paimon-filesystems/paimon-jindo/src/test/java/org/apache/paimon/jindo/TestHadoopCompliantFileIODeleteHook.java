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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@code doDelete} template method in {@link HadoopCompliantFileIO}. */
public class TestHadoopCompliantFileIODeleteHook {

    @Test
    void deleteCallsDoDeleteByDefault() {
        TrackingFileIO fileIO = new TrackingFileIO();
        Path path = new Path("oss://bucket/warehouse/data.parquet");

        try {
            fileIO.delete(path, false);
        } catch (Exception ignored) {
        }

        assertThat(fileIO.doDeleteCalled).isTrue();
    }

    @Test
    void subclassCanOverrideDeleteAndRouteToDoDelete() throws IOException {
        RoutingFileIO fileIO = new RoutingFileIO();
        Path systemPath = new Path("oss://bucket/warehouse/system/data.parquet");
        Path normalPath = new Path("oss://bucket/warehouse/data.parquet");

        // system path → subclass routes to doDelete (real FS delete)
        try {
            fileIO.delete(systemPath, false);
        } catch (Exception ignored) {
        }
        assertThat(fileIO.handledPaths).isEmpty();
        assertThat(fileIO.doDeleteCalled).isTrue();

        // normal path → subclass handles it directly, doDelete not called
        fileIO.doDeleteCalled = false;
        boolean result = fileIO.delete(normalPath, false);
        assertThat(result).isTrue();
        assertThat(fileIO.handledPaths).containsExactly(normalPath);
        assertThat(fileIO.doDeleteCalled).isFalse();
    }

    /** Subclass that overrides delete() to route some paths to doDelete and handle others. */
    private static class RoutingFileIO extends StubFileIO {
        final List<Path> handledPaths = new ArrayList<>();
        boolean doDeleteCalled = false;

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            if (path.toString().contains("system")) {
                return doDelete(path, recursive);
            }
            handledPaths.add(path);
            return true;
        }

        @Override
        protected boolean doDelete(Path path, boolean recursive) throws IOException {
            doDeleteCalled = true;
            return super.doDelete(path, recursive);
        }

        @Override
        public boolean isObjectStore() {
            return true;
        }
    }

    /** Tracks whether doDelete was called. */
    private static class TrackingFileIO extends StubFileIO {
        boolean doDeleteCalled = false;

        @Override
        protected boolean doDelete(Path path, boolean recursive) throws IOException {
            doDeleteCalled = true;
            return super.doDelete(path, recursive);
        }

        @Override
        public boolean isObjectStore() {
            return true;
        }
    }

    /** Minimal stub — no real FS. */
    private abstract static class StubFileIO extends HadoopCompliantFileIO {
        @Override
        public void configure(CatalogContext context) {}

        @Override
        protected Pair<JindoHadoopSystem, String> createFileSystem(
                org.apache.hadoop.fs.Path path, boolean enableCache) {
            throw new UnsupportedOperationException("no real FS in test");
        }
    }
}
