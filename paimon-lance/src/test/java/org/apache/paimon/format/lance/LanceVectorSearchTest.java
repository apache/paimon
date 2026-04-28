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

package org.apache.paimon.format.lance;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.VectorSearchBuilderTest;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

/** Test vector search with Lance file format. */
public class LanceVectorSearchTest extends VectorSearchBuilderTest {

    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString());
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toUri().toString());
        CatalogContext context = CatalogContext.create(options, new TraceableFileIO.Loader(), null);
        catalog = CatalogFactory.createCatalog(context);
        catalog.createDatabase(database, true);
    }

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("vec", new ArrayType(DataTypes.FLOAT()))
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.FILE_FORMAT.key(), "lance")
                .option("test.vector.dimension", "2")
                .option("test.vector.metric", "l2")
                .build();
    }
}
