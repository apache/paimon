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

/**
 * A {@link FileIO} whose io-cache routing can be narrowed: to the origin, or to the files of one
 * table. Call it through {@link IoCacheViews}, which leaves every other FileIO as it is.
 */
public interface IoCacheViewableFileIO extends FileIO {

    /** A view of this FileIO that sends no request to an io-cache target, for mutable files. */
    FileIO originOnly();

    /** A view of this FileIO for one table: io-cache routing only serves that table's files. */
    FileIO withTableView(TableFileIOView view);
}
