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

package org.apache.paimon.fs;

import org.apache.paimon.data.BlobDescriptor;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link PluginFileIO}. */
class PluginFileIOTest {

    @Test
    void testCreateBlobPresignedUrlUsesPluginClassLoader() throws IOException {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        TestPluginFileIO fileIO = new TestPluginFileIO(delegate, pluginClassLoader);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(5);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, "png", validity))
                .thenAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(pluginClassLoader);
                            return "https://example";
                        });

        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "png", validity))
                .isEqualTo("https://example");
        assertThat(fileIO.createdFor).isEqualTo(new Path(descriptor.uri()));
        assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(original);
    }

    private static class TestPluginFileIO extends PluginFileIO {

        private final FileIO delegate;
        private final ClassLoader classLoader;
        private Path createdFor;

        private TestPluginFileIO(FileIO delegate, ClassLoader classLoader) {
            this.delegate = delegate;
            this.classLoader = classLoader;
        }

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            createdFor = path;
            return delegate;
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return classLoader;
        }
    }
}
