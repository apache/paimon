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

import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.rest.RESTTokenFileIO;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that FileIO wrappers preserve native two-phase output streams. */
public class FileIOWrapperTwoPhaseOutputStreamTest {

    private static final Path TARGET = new Path("file:///tmp/two-phase-target");

    @Test
    public void testResolvingFileIOPreservesNativeTwoPhaseStream() throws Exception {
        TestingFileIO delegate = new TestingFileIO();
        TestingResolvingFileIO fileIO = new TestingResolvingFileIO(delegate);

        assertNativeLifecycle(fileIO, delegate);
    }

    @Test
    public void testPluginFileIOPreservesNativeTwoPhaseStream() throws Exception {
        TestingFileIO delegate = new TestingFileIO();
        TestingPluginFileIO fileIO = new TestingPluginFileIO(delegate);

        assertNativeLifecycle(fileIO, delegate);
    }

    @Test
    public void testNestedWrappersResolveCommitterToNativeFileIO() throws Exception {
        TestingFileIO delegate = new TestingFileIO();
        TestingPluginFileIO pluginFileIO = new TestingPluginFileIO(delegate);
        TestingResolvingFileIO resolvingFileIO = new TestingResolvingFileIO(pluginFileIO);

        assertNativeLifecycle(resolvingFileIO, delegate);
    }

    @Test
    public void testRESTTokenAroundResolvingFileIOResolvesCommitter() throws Exception {
        TestingFileIO delegate = new TestingFileIO();
        TestingRESTTokenFileIO fileIO =
                new TestingRESTTokenFileIO(new TestingResolvingFileIO(delegate));

        assertNativeLifecycle(fileIO, delegate);
    }

    @Test
    public void testRESTTokenAroundPluginFileIOResolvesCommitter() throws Exception {
        TestingFileIO delegate = new TestingFileIO();
        TestingRESTTokenFileIO fileIO =
                new TestingRESTTokenFileIO(new TestingPluginFileIO(delegate));

        assertNativeLifecycle(fileIO, delegate);
    }

    private static void assertNativeLifecycle(FileIO wrapper, TestingFileIO delegate)
            throws Exception {
        TwoPhaseOutputStream stream = wrapper.newTwoPhaseOutputStream(TARGET, false);
        assertThat(delegate.nativeStreamCreated).isTrue();

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        committer.commit(wrapper);
        committer.discard(wrapper);
        committer.clean(wrapper);

        assertThat(delegate.commitFileIO).isSameAs(delegate);
        assertThat(delegate.discardFileIO).isSameAs(delegate);
        assertThat(delegate.cleanFileIO).isSameAs(delegate);
    }

    private static class TestingResolvingFileIO extends ResolvingFileIO {

        private static final long serialVersionUID = 1L;

        private final FileIO delegate;

        private TestingResolvingFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public FileIO fileIO(Path path) {
            return delegate;
        }
    }

    private static class TestingPluginFileIO extends PluginFileIO {

        private static final long serialVersionUID = 1L;

        private final FileIO delegate;

        private TestingPluginFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            return delegate;
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return getClass().getClassLoader();
        }

        @Override
        public boolean isObjectStore() {
            return true;
        }
    }

    private static class TestingRESTTokenFileIO extends RESTTokenFileIO {

        private static final long serialVersionUID = 1L;

        private final FileIO delegate;

        private TestingRESTTokenFileIO(FileIO delegate) {
            super(null, null, null, null);
            this.delegate = delegate;
        }

        @Override
        public FileIO fileIO() {
            return delegate;
        }
    }

    private static class TestingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private boolean nativeStreamCreated;
        private FileIO commitFileIO;
        private FileIO discardFileIO;
        private FileIO cleanFileIO;

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite) {
            return new TestingPositionOutputStream();
        }

        @Override
        public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite) {
            nativeStreamCreated = true;
            return new TestingTwoPhaseOutputStream(this);
        }
    }

    private static class TestingPositionOutputStream extends PositionOutputStream {

        private long position;

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int value) {
            position++;
        }

        @Override
        public void write(byte[] bytes) {
            position += bytes.length;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            position += length;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    private static class TestingTwoPhaseOutputStream extends TwoPhaseOutputStream {

        private final TestingFileIO owner;
        private final TestingPositionOutputStream delegate = new TestingPositionOutputStream();

        private TestingTwoPhaseOutputStream(TestingFileIO owner) {
            this.owner = owner;
        }

        @Override
        public Committer closeForCommit() {
            return new TestingCommitter(owner);
        }

        @Override
        public long getPos() {
            return delegate.getPos();
        }

        @Override
        public void write(int value) {
            delegate.write(value);
        }

        @Override
        public void write(byte[] bytes) {
            delegate.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            delegate.write(bytes, offset, length);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    private static class TestingCommitter implements TwoPhaseOutputStream.Committer {

        private static final long serialVersionUID = 1L;

        private final TestingFileIO owner;

        private TestingCommitter(TestingFileIO owner) {
            this.owner = owner;
        }

        @Override
        public void commit(FileIO fileIO) {
            owner.commitFileIO = fileIO;
        }

        @Override
        public void discard(FileIO fileIO) {
            owner.discardFileIO = fileIO;
        }

        @Override
        public Path targetPath() {
            return TARGET;
        }

        @Override
        public void clean(FileIO fileIO) throws IOException {
            owner.cleanFileIO = fileIO;
        }
    }
}
