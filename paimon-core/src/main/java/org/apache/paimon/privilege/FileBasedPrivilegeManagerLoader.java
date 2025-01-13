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

package org.apache.paimon.privilege;

import org.apache.paimon.fs.FileIO;

/** Loader for creating a {@link FileBasedPrivilegeManager}. */
public class FileBasedPrivilegeManagerLoader implements PrivilegeManagerLoader {

    private static final long serialVersionUID = 1L;

    private final String warehouse;
    private final FileIO fileIO;
    private final String user;
    private final String password;

    public FileBasedPrivilegeManagerLoader(
            String warehouse, FileIO fileIO, String user, String password) {
        this.warehouse = warehouse;
        this.fileIO = fileIO;
        this.user = user;
        this.password = password;
    }

    @Override
    public FileBasedPrivilegeManager load() {
        return new FileBasedPrivilegeManager(warehouse, fileIO, user, password);
    }
}
