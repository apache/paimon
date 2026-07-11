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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;

/** Selects data files whose complete rows can be used to build vector index payloads. */
public final class PkVectorSourcePolicy {

    private PkVectorSourcePolicy() {}

    public static boolean shouldWrite(FileSource fileSource, int level) {
        return fileSource == FileSource.COMPACT && level > 0;
    }

    public static boolean shouldRead(DataFileMeta file) {
        return file.fileSource()
                .map(fileSource -> shouldWrite(fileSource, file.level()))
                .orElse(false);
    }
}
