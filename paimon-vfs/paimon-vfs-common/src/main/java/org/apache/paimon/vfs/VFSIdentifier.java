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

package org.apache.paimon.vfs;

/** Identifier for virtual path. */
public abstract class VFSIdentifier {
    enum VFSFileType {
        CATALOG, // pas://catalog/
        DATABASE, // pas://catalog/database/
        TABLE, // pas://catalog/database/table/
        TABLE_OBJECT // pas://catalog/database/table/file.txt
    }

    protected VFSFileType vfsFileType;
    protected String databaseName;

    public VFSIdentifier(VFSFileType vfsFileType, String databaseName) {
        this.vfsFileType = vfsFileType;
        this.databaseName = databaseName;
    }

    public VFSFileType getVfsFileType() {
        return vfsFileType;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
