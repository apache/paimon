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

package org.apache.paimon.spark.procedure.actions;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.procedure.actions.file.SuccessFile;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;

/** A {@link PartitionMarkDoneAction} which create "_SUCCESS" file. */
public class SuccessFileMarkDoneAction implements PartitionMarkDoneAction {

    public static final String SUCCESS_FILE_NAME = "_SUCCESS";

    private final FileIO fileIO;
    private final Path tablePath;

    public SuccessFileMarkDoneAction(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    @Override
    public void markDone(String partition) throws Exception {
        Path partitionPath = new Path(tablePath, partition);
        Path successPath = new Path(partitionPath, SUCCESS_FILE_NAME);

        long currentTime = System.currentTimeMillis();
        SuccessFile successFile = SuccessFile.safelyFromPath(fileIO, successPath);
        if (successFile == null) {
            successFile = new SuccessFile(currentTime, currentTime);
        } else {
            successFile = successFile.updateModificationTime(currentTime);
        }
        fileIO.overwriteFileUtf8(successPath, successFile.toJson());
    }

    @Nullable
    public static SuccessFile safelyFromPath(FileIO fileIO, Path path) throws IOException {
        try {
            String json = fileIO.readFileUtf8(path);
            return SuccessFile.fromJson(json);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public void close() {}
}
