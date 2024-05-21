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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.table.source.DeletionFile;

import java.util.Objects;

/** a relative data file path and its associated DeletionFile object. */
public class DeletionFileWithDataFile {

    private final String dataFile;

    private final DeletionFile deletionFile;

    public DeletionFileWithDataFile(String dataFile, DeletionFile deletionFile) {
        this.dataFile = dataFile;
        this.deletionFile = deletionFile;
    }

    public String dataFile() {
        return this.dataFile;
    }

    public DeletionFile deletionFile() {
        return this.deletionFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeletionFileWithDataFile that = (DeletionFileWithDataFile) o;
        return dataFile.equals(that.dataFile) && Objects.equals(deletionFile, that.deletionFile);
    }
}
