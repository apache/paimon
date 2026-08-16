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

package org.apache.paimon.table.source;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compaction-visible data files and full-text payloads for one snapshot bucket. */
public class PrimaryKeyFullTextSearchSplit extends FullTextSearchSplit {

    private static final long serialVersionUID = 1L;
    private static final int VERSION = 1;

    private DataSplit dataSplit;
    private transient List<IndexFileMeta> payloadFiles;
    private List<String> uncoveredDataFiles;

    public PrimaryKeyFullTextSearchSplit(
            DataSplit dataSplit,
            List<IndexFileMeta> payloadFiles,
            List<String> uncoveredDataFiles) {
        checkArgument(
                !dataSplit.isStreaming(), "Primary-key full-text search requires a batch split.");
        Set<String> sourceFiles = new HashSet<>();
        for (DataFileMeta dataFile : dataSplit.dataFiles()) {
            checkArgument(
                    sourceFiles.add(dataFile.fileName()),
                    "Data file %s appears more than once in a full-text bucket split.",
                    dataFile.fileName());
        }

        Set<String> covered = new HashSet<>();
        for (IndexFileMeta payload : payloadFiles) {
            boolean coversActiveSource = false;
            for (PrimaryKeyIndexSourceFile source :
                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFiles()) {
                if (!sourceFiles.contains(source.fileName())) {
                    continue;
                }
                coversActiveSource = true;
                checkArgument(
                        covered.add(source.fileName()),
                        "Data file %s is covered by more than one full-text payload.",
                        source.fileName());
            }
            checkArgument(
                    coversActiveSource,
                    "Full-text payload %s does not cover an active data file in its bucket split.",
                    payload.fileName());
        }

        Set<String> uncovered = new HashSet<>();
        for (String source : uncoveredDataFiles) {
            checkArgument(
                    sourceFiles.contains(source),
                    "Uncovered full-text data file %s is outside its bucket split.",
                    source);
            checkArgument(
                    uncovered.add(source),
                    "Uncovered full-text data file %s appears more than once.",
                    source);
            checkArgument(
                    !covered.contains(source),
                    "Data file %s cannot be both indexed and uncovered.",
                    source);
        }
        checkArgument(
                covered.size() + uncovered.size() == sourceFiles.size(),
                "Every full-text source file must be indexed or explicitly uncovered.");

        this.dataSplit = dataSplit;
        this.payloadFiles = Collections.unmodifiableList(new ArrayList<>(payloadFiles));
        this.uncoveredDataFiles = Collections.unmodifiableList(new ArrayList<>(uncoveredDataFiles));
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    public List<IndexFileMeta> payloadFiles() {
        return payloadFiles;
    }

    public List<String> uncoveredDataFiles() {
        return uncoveredDataFiles;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(VERSION);
        out.writeInt(payloadFiles.size());
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        for (IndexFileMeta payloadFile : payloadFiles) {
            serializer.serialize(payloadFile, new DataOutputViewStreamWrapper(out));
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported PrimaryKeyFullTextSearchSplit version: " + version);
        }
        int payloadFileCount = in.readInt();
        if (payloadFileCount < 0) {
            throw new IOException("Negative primary-key full-text payload file count.");
        }
        List<IndexFileMeta> payloads = new ArrayList<>(payloadFileCount);
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        for (int i = 0; i < payloadFileCount; i++) {
            payloads.add(serializer.deserialize(new DataInputViewStreamWrapper(in)));
        }
        this.payloadFiles = Collections.unmodifiableList(payloads);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrimaryKeyFullTextSearchSplit that = (PrimaryKeyFullTextSearchSplit) o;
        return Objects.equals(dataSplit, that.dataSplit)
                && Objects.equals(payloadFiles, that.payloadFiles)
                && Objects.equals(uncoveredDataFiles, that.uncoveredDataFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSplit, payloadFiles, uncoveredDataFiles);
    }
}
