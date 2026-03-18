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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

/** Factory for creating bitmap global indexers. */
public class BitmapGlobalIndexerFactory implements GlobalIndexerFactory {

    public static final String IDENTIFIER = "bitmap";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public GlobalIndexer create(DataField dataField, Options options) {
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(dataField.type(), options);
        return new BitmapGlobalIndex(bitmapFileIndex);
    }
}
