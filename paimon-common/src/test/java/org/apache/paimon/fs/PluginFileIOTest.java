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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .thenAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(pluginClassLoader);
                            return "https://example";
                        });

        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .isEqualTo("https://example");
        assertThat(fileIO.createdFor).isEqualTo(new Path(descriptor.uri()));
        assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(original);
    }

    @Test
    void testCloseReleasesTheDelegateUnderThePluginClassLoader() throws IOException {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        TestPluginFileIO fileIO = new TestPluginFileIO(delegate, pluginClassLoader);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        doAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(pluginClassLoader);
                            return null;
                        })
                .when(delegate)
                .close();

        // nothing has been resolved yet, so there is nothing to release
        new TestPluginFileIO(delegate, pluginClassLoader).close();
        verify(delegate, never()).close();

        fileIO.exists(new Path("oss://bucket/table/file"));
        fileIO.close();

        verify(delegate).close();
        assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(original);

        // the delegate has been dropped, closing again must not touch it a second time
        fileIO.close();
        verify(delegate).close();
    }

    @Test
    void testUseAfterCloseFailsWithIOExceptionRatherThanNpe() throws IOException {
        FileIO delegate = mock(FileIO.class);
        TestPluginFileIO fileIO = new TestPluginFileIO(delegate, new ClassLoader() {});
        Path path = new Path("oss://bucket/table/file");

        fileIO.exists(path);
        fileIO.close();

        // close() is the only writer of null, so a plain field read here would NPE, and silently
        // re-creating the delegate would build a second file system nobody releases
        assertThatThrownBy(() -> fileIO.exists(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        assertThat(fileIO.createdCount).isEqualTo(1);
    }

    private static class TestPluginFileIO extends PluginFileIO {

        private final FileIO delegate;
        private final ClassLoader classLoader;
        private Path createdFor;
        private int createdCount;

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
            createdCount++;
            return delegate;
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return classLoader;
        }
    }
}
