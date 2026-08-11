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

package org.apache.paimon.s3;

import org.apache.paimon.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3AtomicWriteTest {

    @Test
    void testExistingTargetReturnsFalseAndCleansTemporaryFile() throws IOException {
        Path target = new Path("s3://bucket/path/file");
        FailingRenameFileIO fileIO =
                new FailingRenameFileIO(new FileAlreadyExistsException(target.toString()));

        assertThat(fileIO.tryToWriteAtomic(target, "replacement")).isFalse();
        assertThat(fileIO.renameDestination).isEqualTo(target);
        assertThat(fileIO.deletedPath).isEqualTo(fileIO.renameSource);
    }

    @Test
    void testUnrelatedRenameFailureStillPropagatesAndCleansTemporaryFile() throws IOException {
        Path target = new Path("s3://bucket/path/file");
        IOException failure = new IOException("rename failed");
        FailingRenameFileIO fileIO = new FailingRenameFileIO(failure);

        assertThatThrownBy(() -> fileIO.tryToWriteAtomic(target, "replacement")).isSameAs(failure);
        assertThat(fileIO.renameDestination).isEqualTo(target);
        assertThat(fileIO.deletedPath).isEqualTo(fileIO.renameSource);
    }

    private static class FailingRenameFileIO extends S3FileIO {

        private final FileSystem fileSystem;
        private final IOException renameFailure;
        private Path renameSource;
        private Path renameDestination;
        private Path deletedPath;

        private FailingRenameFileIO(IOException renameFailure) throws IOException {
            this.renameFailure = renameFailure;
            fileSystem =
                    new FilterFileSystem(FileSystem.getLocal(new Configuration())) {
                        @Override
                        public boolean rename(
                                org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst)
                                throws IOException {
                            renameSource = new Path(src.toUri());
                            renameDestination = new Path(dst.toUri());
                            throw FailingRenameFileIO.this.renameFailure;
                        }
                    };
        }

        @Override
        public void writeFile(Path path, String content, boolean overwrite) {}

        @Override
        protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
            return fileSystem;
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            deletedPath = path;
            return true;
        }
    }
}
