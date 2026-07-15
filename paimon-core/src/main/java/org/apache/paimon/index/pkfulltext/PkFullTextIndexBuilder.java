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

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Opens projected data-file readers and builds an ordered-source full-text archive. */
public class PkFullTextIndexBuilder {

    private final PkFullTextIndexFile indexFile;
    private final PkFullTextDataFileReader.Factory readerFactory;
    private final DataField textField;
    private final Options indexOptions;
    private final String definitionFingerprint;

    public PkFullTextIndexBuilder(
            PkFullTextIndexFile indexFile,
            PkFullTextDataFileReader.Factory readerFactory,
            DataField textField,
            Options indexOptions,
            String definitionFingerprint) {
        this.indexFile = indexFile;
        this.readerFactory = readerFactory;
        this.textField = textField;
        this.indexOptions = indexOptions;
        this.definitionFingerprint = definitionFingerprint;
    }

    public IndexFileMeta build(DataFileMeta sourceFile) throws IOException {
        PkFullTextDataFileReader reader = readerFactory.create(sourceFile);
        return indexFile.build(sourceFile, reader, textField, indexOptions, definitionFingerprint);
    }

    public IndexFileMeta build(List<DataFileMeta> sourceFiles) throws IOException {
        List<PkFullTextIndexFile.Source> sources = new ArrayList<>(sourceFiles.size());
        try {
            for (DataFileMeta sourceFile : sourceFiles) {
                sources.add(
                        new PkFullTextIndexFile.Source(
                                sourceFile, readerFactory.create(sourceFile)));
            }
        } catch (IOException | RuntimeException | Error failure) {
            for (PkFullTextIndexFile.Source source : sources) {
                source.close();
            }
            throw failure;
        }
        return indexFile.build(sources, textField, indexOptions, definitionFingerprint);
    }
}
