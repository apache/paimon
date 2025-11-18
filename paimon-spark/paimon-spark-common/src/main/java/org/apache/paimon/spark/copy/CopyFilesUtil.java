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

package org.apache.paimon.spark.copy;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.Preconditions;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Utils for copy files. */
public class CopyFilesUtil {

    public static void copyFiles(
            FileIO sourceFileIO,
            FileIO targetFileIO,
            Path sourcePath,
            Path targetPath,
            boolean overwrite)
            throws IOException {
        try (SeekableInputStream is = sourceFileIO.newInputStream(sourcePath);
                PositionOutputStream os = targetFileIO.newOutputStream(targetPath, overwrite)) {
            IOUtils.copy(is, os);
        }
    }

    public static List<CopyFileInfo> toCopyFileInfos(List<Path> fileList, Path sourceTableRoot) {
        List<CopyFileInfo> result = new ArrayList<>();
        for (Path file : fileList) {
            Path relativePath = getPathExcludeTableRoot(file, sourceTableRoot);
            result.add(new CopyFileInfo(file.toUri().toString(), relativePath.toString()));
        }
        return result;
    }

    public static Path getPathExcludeTableRoot(Path absolutePath, Path sourceTableRoot) {
        String fileAbsolutePath = absolutePath.toUri().toString();
        String sourceTableRootPath = sourceTableRoot.toString();

        Preconditions.checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "File absolute path does not start with source table root path. This is unexpected. "
                        + "fileAbsolutePath is: "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is: "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
    }
}
