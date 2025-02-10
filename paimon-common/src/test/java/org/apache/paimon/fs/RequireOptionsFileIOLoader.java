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

package org.apache.paimon.fs;

import org.apache.paimon.fs.local.LocalFileIO;

import java.util.ArrayList;
import java.util.List;

/** Test {@link FileIOLoader}. */
public class RequireOptionsFileIOLoader implements FileIOLoader {

    private static final long serialVersionUID = 1L;

    @Override
    public String getScheme() {
        return "require-options";
    }

    @Override
    public List<String[]> requiredOptions() {
        List<String[]> options = new ArrayList<>();
        options.add(new String[] {"Require1", "Re-quire1"});
        options.add(new String[] {"reQuire2"});
        return options;
    }

    @Override
    public FileIO load(Path path) {
        return new MyFileIO();
    }

    /** My {@link LocalFileIO} for checking. */
    public static class MyFileIO extends LocalFileIO {}
}
