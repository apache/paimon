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

/** Applies io-cache views to a FileIO that supports them, and returns any other FileIO as is. */
public final class IoCacheViews {

    private IoCacheViews() {}

    /** See {@link IoCacheViewableFileIO#originOnly()}. */
    public static FileIO originOnly(FileIO fileIO) {
        return fileIO instanceof IoCacheViewableFileIO
                ? ((IoCacheViewableFileIO) fileIO).originOnly()
                : fileIO;
    }

    /** See {@link IoCacheViewableFileIO#withTableView(TableFileIOView)}. */
    public static FileIO withTableView(FileIO fileIO, TableFileIOView view) {
        return fileIO instanceof IoCacheViewableFileIO
                ? ((IoCacheViewableFileIO) fileIO).withTableView(view)
                : fileIO;
    }
}
