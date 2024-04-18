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

package org.apache.paimon.clone;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Utility class for copy file. */
public class CopyFileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFileUtils.class);

    public static void copyFile(
            CloneFileInfo cloneFileInfo,
            FileIO sourceTableFileIO,
            FileIO targetTableFileIO,
            Path sourceTableRootPath,
            Path targetTableRootPath)
            throws IOException {
        Path filePathExcludeTableRoot = cloneFileInfo.getFilePathExcludeTableRoot();
        Path sourcePath = new Path(sourceTableRootPath.toString() + filePathExcludeTableRoot);
        Path targetPath = new Path(targetTableRootPath.toString() + filePathExcludeTableRoot);

        LOG.debug("Begin copy file from {} to {}.", sourcePath, targetPath);
        IOUtils.copyBytes(
                sourceTableFileIO.newInputStream(sourcePath),
                targetTableFileIO.newOutputStream(targetPath, true));
        LOG.debug("End copy file from {} to {}.", sourcePath, targetPath);
    }
}
