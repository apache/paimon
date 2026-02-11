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

package org.apache.paimon.jindodls;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.jindo.JindoFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for DLS {@link FileIO}. */
public class DLSFileIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDLSFileIO() throws Exception {
        Options options = new Options();
        FileIO fileIO =
                FileIO.get(new Path("dls://" + tempDir.toString()), CatalogContext.create(options));
        assertThat(fileIO).isInstanceOf(JindoFileIO.class);
    }
}
