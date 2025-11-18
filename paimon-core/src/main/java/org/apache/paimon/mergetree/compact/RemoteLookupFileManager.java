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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.LookupLevels.RemoteFileDownloader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Manager to manage remote files for lookup. */
public class RemoteLookupFileManager<T> implements RemoteFileDownloader {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final TableSchema schema;
    private final LookupLevels<T> lookupLevels;
    private final int levelThreshold;
    private final SchemaManager schemaManager;
    private final Map<Long, RowType> schemaRowTypes;

    public RemoteLookupFileManager(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            TableSchema schema,
            LookupLevels<T> lookupLevels,
            SchemaManager schemaManager,
            int levelThreshold) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.schema = schema;
        this.lookupLevels = lookupLevels;
        this.levelThreshold = levelThreshold;
        this.lookupLevels.setRemoteFileDownloader(this);
        this.schemaManager = schemaManager;
        this.schemaRowTypes = new HashMap<>();
    }

    public DataFileMeta genRemoteLookupFile(DataFileMeta file) throws IOException {
        if (file.level() < levelThreshold) {
            return file;
        }

        if (remoteSst(file).isPresent()) {
            // ignore existed
            return file;
        }

        LookupFile lookupFile = lookupLevels.createLookupFile(file);
        long length = lookupFile.localFile().length();
        String remoteSstName = newRemoteSstName(file, length);
        Path sstFile = remoteSstPath(file, remoteSstName);
        try (FileInputStream is = new FileInputStream(lookupFile.localFile());
                PositionOutputStream os = fileIO.newOutputStream(sstFile, false)) {
            IOUtils.copy(is, os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        lookupLevels.addLocalFile(file, lookupFile);
        List<String> extraFiles = new ArrayList<>(file.extraFiles());
        extraFiles.add(remoteSstName);
        return file.copy(extraFiles);
    }

    @Override
    public boolean tryToDownload(DataFileMeta dataFile, File localFile) {
        long schemaId = dataFile.schemaId();
        if (schemaId != schema.id()) {
            if (!schemaRowTypes.containsKey(schemaId)) {
                schemaRowTypes.put(schemaId, schemaManager.schema(schemaId).logicalRowType());
            }
            if (!schema.logicalRowType().equals(schemaRowTypes.get(schemaId))) {
                return false;
            }
        }

        Optional<String> remoteSst = remoteSst(dataFile);
        if (remoteSst.isPresent()) {
            Path remoteSstPath = remoteSstPath(dataFile, remoteSst.get());
            try (SeekableInputStream is = fileIO.newInputStream(remoteSstPath);
                    FileOutputStream os = new FileOutputStream(localFile)) {
                IOUtils.copy(is, os);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
        return false;
    }

    private Optional<String> remoteSst(DataFileMeta file) {
        return file.extraFiles().stream()
                .filter(f -> f.endsWith(lookupLevels.remoteSstSuffix()))
                .findFirst();
    }

    private String newRemoteSstName(DataFileMeta file, long length) {
        return file.fileName() + "." + length + lookupLevels.remoteSstSuffix();
    }

    private Path remoteSstPath(DataFileMeta file, String remoteSstName) {
        return new Path(pathFactory.toPath(file).getParent(), remoteSstName);
    }
}
