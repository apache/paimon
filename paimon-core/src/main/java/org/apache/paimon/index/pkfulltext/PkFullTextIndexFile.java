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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds one immutable full-text archive from ordered compact primary-key data files. */
public class PkFullTextIndexFile extends IndexFile {

    public static final String INDEX_TYPE = "full-text";

    public PkFullTextIndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public IndexFileMeta build(
            DataFileMeta sourceFile,
            PkFullTextDataFileReader textReader,
            DataField textField,
            Options indexOptions)
            throws IOException {
        GlobalIndexer indexer;
        try {
            indexer = GlobalIndexer.create(INDEX_TYPE, textField, indexOptions);
        } catch (RuntimeException | Error failure) {
            IOUtils.closeQuietly(textReader);
            throw failure;
        }
        return build(
                Collections.singletonList(new Source(sourceFile, textReader)), textField, indexer);
    }

    public IndexFileMeta build(List<Source> sources, DataField textField, Options indexOptions)
            throws IOException {
        GlobalIndexer indexer;
        try {
            indexer = GlobalIndexer.create(INDEX_TYPE, textField, indexOptions);
        } catch (RuntimeException | Error failure) {
            closeSources(sources);
            throw failure;
        }
        return build(sources, textField, indexer);
    }

    IndexFileMeta build(
            DataFileMeta sourceFile,
            PkFullTextDataFileReader textReader,
            DataField textField,
            GlobalIndexer indexer)
            throws IOException {
        return build(
                Collections.singletonList(new Source(sourceFile, textReader)), textField, indexer);
    }

    IndexFileMeta build(List<Source> sources, DataField textField, GlobalIndexer indexer)
            throws IOException {
        checkArgument(!sources.isEmpty(), "A full-text archive must reference source files.");
        int dataLevel = sources.get(0).sourceFile.level();
        checkArgument(dataLevel > 0, "A full-text archive requires a positive data level.");
        long totalRowCount = 0;
        List<PrimaryKeyIndexSourceFile> sourceFiles = new ArrayList<>(sources.size());
        for (Source source : sources) {
            checkArgument(
                    source.sourceFile.level() == dataLevel,
                    "A full-text archive cannot mix data levels %s and %s.",
                    dataLevel,
                    source.sourceFile.level());
            checkArgument(
                    source.sourceFile.rowCount() > 0, "Full-text source file must contain rows.");
            totalRowCount = Math.addExact(totalRowCount, source.sourceFile.rowCount());
            sourceFiles.add(
                    new PrimaryKeyIndexSourceFile(
                            source.sourceFile.fileName(), source.sourceFile.rowCount()));
        }

        ArchiveFileWriter fileWriter = new ArchiveFileWriter();
        GlobalIndexWriter writer = null;
        boolean success = false;
        try {
            writer = indexer.createWriter(fileWriter);
            checkArgument(
                    writer instanceof GlobalIndexSingleColumnWriter,
                    "Full-text indexer must create a single-column writer.");
            GlobalIndexSingleColumnWriter singleColumnWriter =
                    (GlobalIndexSingleColumnWriter) writer;
            long sourceOffset = 0;
            for (Source source : sources) {
                checkArgument(
                        source.textReader.rowCount() == source.sourceFile.rowCount(),
                        "Text row count %s does not match source file %s row count %s.",
                        source.textReader.rowCount(),
                        source.sourceFile.fileName(),
                        source.sourceFile.rowCount());
                for (long rowPosition = 0;
                        rowPosition < source.sourceFile.rowCount();
                        rowPosition++) {
                    singleColumnWriter.write(
                            source.textReader.readNextText(), sourceOffset + rowPosition);
                }
                sourceOffset += source.sourceFile.rowCount();
            }
            List<ResultEntry> results = writer.finish();
            checkArgument(
                    results.size() == 1,
                    "Full-text build must produce exactly one archive, but produced %s.",
                    results.size());
            checkArgument(
                    fileWriter.createdFiles.size() == 1,
                    "Full-text build must allocate exactly one archive, but allocated %s.",
                    fileWriter.createdFiles.size());
            ResultEntry result = results.get(0);
            checkArgument(
                    result.rowCount() == totalRowCount,
                    "Full-text archive row count %s does not match source row count %s.",
                    result.rowCount(),
                    totalRowCount);
            Path archivePath = fileWriter.path(result.fileName());
            byte[] archiveMetadata = result.meta() == null ? new byte[0] : result.meta();
            IndexFileMeta archive =
                    new IndexFileMeta(
                            INDEX_TYPE,
                            result.fileName(),
                            fileIO.getFileSize(archivePath),
                            result.rowCount(),
                            new GlobalIndexMeta(
                                    0,
                                    totalRowCount - 1,
                                    textField.id(),
                                    null,
                                    archiveMetadata,
                                    new PrimaryKeyIndexSourceMeta(dataLevel, sourceFiles)
                                            .serialize()),
                            pathFactory.isExternalPath() ? archivePath.toString() : null);
            success = true;
            return archive;
        } finally {
            for (Source source : sources) {
                IOUtils.closeQuietly(source.textReader);
            }
            if (writer instanceof AutoCloseable) {
                IOUtils.closeQuietly((AutoCloseable) writer);
            }
            if (!success) {
                fileWriter.deleteCreatedFiles();
            }
        }
    }

    private static void closeSources(List<Source> sources) {
        for (Source source : sources) {
            IOUtils.closeQuietly(source.textReader);
        }
    }

    /** One data-file source whose row positions are appended to an archive. */
    public static class Source {

        private final DataFileMeta sourceFile;
        private final PkFullTextDataFileReader textReader;

        public Source(DataFileMeta sourceFile, PkFullTextDataFileReader textReader) {
            this.sourceFile = sourceFile;
            this.textReader = textReader;
        }

        void close() {
            IOUtils.closeQuietly(textReader);
        }
    }

    private class ArchiveFileWriter implements GlobalIndexFileWriter {

        private final Map<String, Path> createdFiles = new HashMap<>();

        @Override
        public String newFileName(String prefix) {
            Path path = pathFactory.newPath();
            createdFiles.put(path.getName(), path);
            return path.getName();
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) throws IOException {
            return fileIO.newOutputStream(path(fileName), false);
        }

        private Path path(String fileName) {
            Path path = createdFiles.get(fileName);
            checkArgument(path != null, "Full-text archive %s was not allocated.", fileName);
            return path;
        }

        private void deleteCreatedFiles() {
            for (Path path : createdFiles.values()) {
                fileIO.deleteQuietly(path);
            }
        }
    }
}
