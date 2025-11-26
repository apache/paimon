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

package org.apache.paimon.spark.globalindex.bitmap;

import org.apache.paimon.spark.globalindex.GlobalIndexBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Builder for creating bitmap-based global indexes.
 *
 * <p>This implementation does not apply any custom transformations to the input dataset, allowing
 * the data to be processed as-is for bitmap index creation.
 */
public class BitmapGlobalIndexBuilder extends GlobalIndexBuilder {

    protected BitmapGlobalIndexBuilder(GlobalIndexBuilderContext context) {
        super(context);
    }

    @Override
    public Dataset<Row> customTopo(Dataset<Row> input) {
        // No custom transformation needed for bitmap index
        return input;
    }
}
