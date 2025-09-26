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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.FormatTableRollingFileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** TwoPhaseCommit directly sink data to file. */
public class TwoPhaseCommitDirectSinkWriter {

    private final Supplier<FormatTableRollingFileWriter> writerSupplier;

    private FormatTableRollingFileWriter writer;

    public TwoPhaseCommitDirectSinkWriter(Supplier<FormatTableRollingFileWriter> writerSupplier) {
        this.writerSupplier = writerSupplier;
    }

    public boolean write(InternalRow data) throws IOException {
        if (writer == null) {
            writer = writerSupplier.get();
        }
        writer.write(data);
        return true;
    }

    public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws IOException {
        List<TwoPhaseOutputStream.Committer> commits = new ArrayList<>();

        if (writer != null) {
            writer.close();
            commits.addAll(writer.committers());
            writer = null;
        }
        return commits;
    }

    public void close() {
        if (writer != null) {
            writer.abort();
            writer = null;
        }
    }
}
