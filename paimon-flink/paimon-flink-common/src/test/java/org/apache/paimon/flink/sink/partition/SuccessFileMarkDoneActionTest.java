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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.actions.SuccessFileMarkDoneAction;
import org.apache.paimon.partition.file.SuccessFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class SuccessFileMarkDoneActionTest {

    @TempDir java.nio.file.Path temp;

    @Test
    public void test() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path path = new Path(temp.toUri());
        SuccessFileMarkDoneAction action = new SuccessFileMarkDoneAction(fileIO, path);
        Path successPath = new Path(path, "dt=20240513/_SUCCESS");

        action.markDone("dt=20240513");
        SuccessFile successFile1 = SuccessFile.safelyFromPath(fileIO, successPath);
        assertThat(successFile1).isNotNull();

        Thread.sleep(100);
        action.markDone("dt=20240513");
        SuccessFile successFile2 = SuccessFile.safelyFromPath(fileIO, successPath);
        assertThat(successFile2).isNotNull();

        assertThat(successFile1.creationTime() == successFile2.creationTime()).isTrue();
        assertThat(successFile1.modificationTime() < successFile2.modificationTime()).isTrue();
    }
}
