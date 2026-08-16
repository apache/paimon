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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Manager to manage remote files for lookup. */
public class RemoteLookupFileManager<T> implements RemoteFileDownloader {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteLookupFileManager.class);

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final LookupLevels<T> lookupLevels;
    private final int levelThreshold;

    public RemoteLookupFileManager(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            LookupLevels<T> lookupLevels,
            int levelThreshold) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.lookupLevels = lookupLevels;
        this.levelThreshold = levelThreshold;
        this.lookupLevels.setRemoteFileDownloader(this);
    }

    public DataFileMeta genRemoteLookupFile(DataFileMeta file) throws IOException {
        if (file.level() < levelThreshold) {
            return file;
        }

        if (lookupLevels.remoteSst(file).isPresent()) {
            // ignore existed
            return file;
        }

        LookupFile lookupFile = lookupLevels.createLookupFile(file);
        long length = lookupFile.localFile().length();
        String remoteSstName = lookupLevels.newRemoteSst(file, length);
        Path sstFile = remoteSstPath(file, remoteSstName);
        try (FileInputStream is = new FileInputStream(lookupFile.localFile());
                PositionOutputStream os = fileIO.newOutputStream(sstFile, true)) {
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
    public boolean tryToDownload(DataFileMeta dataFile, String remoteSstFile, File localFile) {
        Path remoteSstPath = remoteSstPath(dataFile, remoteSstFile);
        try (SeekableInputStream is = fileIO.newInputStream(remoteSstPath);
                FileOutputStream os = new FileOutputStream(localFile)) {
            IOUtils.copy(is, os);
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to download remote lookup file {}, skipping.", remoteSstPath, e);
            return false;
        }
    }

    private Path remoteSstPath(DataFileMeta file, String remoteSstName) {
        return new Path(pathFactory.toPath(file).getParent(), remoteSstName);
    }
}
