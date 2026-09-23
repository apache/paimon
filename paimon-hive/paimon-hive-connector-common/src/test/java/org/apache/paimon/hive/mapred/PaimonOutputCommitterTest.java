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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.sink.CommitMessage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonOutputCommitter}. */
public class PaimonOutputCommitterTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void readPreCommitFileReturnsEmptyWhenFileMissingAndIgnoreMissing() throws Exception {
        Path missing = new Path(folder.newFolder().getAbsolutePath(), "task_0.preCommit");
        List<CommitMessage> result = invokeReadPreCommitFile(missing, true);
        assertThat(result).isEmpty();
    }

    @Test
    public void readPreCommitFileThrowsWhenFileMissingAndStrict() throws Exception {
        Path missing = new Path(folder.newFolder().getAbsolutePath(), "task_0.preCommit");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> invokeReadPreCommitFile(missing, false))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("missing during commit");
    }

    @Test
    public void readPreCommitFileThrowsOnCorruptFile() throws Exception {
        java.io.File f = folder.newFile("task_0.preCommit");
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(f)) {
            fos.write(new byte[] {0x00, 0x01, 0x02, 0x03});
        }
        Path corrupt = new Path(f.getAbsolutePath());
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> invokeReadPreCommitFile(corrupt, true))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Can not read or parse CommitMessage file");
    }

    @Test
    public void readPreCommitFileReturnsListForValidFile() throws Exception {
        java.io.File f = folder.newFile("task_0.preCommit");
        try (ObjectOutputStream oos = new ObjectOutputStream(new java.io.FileOutputStream(f))) {
            oos.writeObject(Collections.<CommitMessage>emptyList());
        }
        Path written = new Path(f.getAbsolutePath());
        List<CommitMessage> result = invokeReadPreCommitFile(written, false);
        assertThat(result).isNotNull().isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static List<CommitMessage> invokeReadPreCommitFile(Path location, boolean ignoreMissing)
            throws Exception {
        Method m =
                PaimonOutputCommitter.class.getDeclaredMethod(
                        "readPreCommitFile",
                        Path.class,
                        org.apache.paimon.fs.FileIO.class,
                        boolean.class);
        m.setAccessible(true);
        try {
            return (List<CommitMessage>)
                    m.invoke(null, location, LocalFileIO.create(), ignoreMissing);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }
}
