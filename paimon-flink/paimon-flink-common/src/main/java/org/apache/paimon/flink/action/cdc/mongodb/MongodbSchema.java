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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;

/**
 * Represents the schema of a MongoDB collection.
 *
 * <p>This class provides methods to retrieve and manage the schema details of a MongoDB collection,
 * including the database name, table (collection) name, fields, and primary keys. The schema can be
 * acquired in two modes: SPECIFIED and DYNAMIC. In the SPECIFIED mode, the schema details are
 * provided explicitly, while in the DYNAMIC mode, the schema is inferred from the first document in
 * the collection.
 *
 * <p>The class also provides utility methods to generate schema fields and create a new MongoDB
 * schema instance.
 */
public class MongodbSchema {

    private static final String ID_FIELD = "_id";
    private final String databaseName;
    private final String tableName;
    private final LinkedHashMap<String, DataType> fields;
    private final List<String> primaryKeys;

    public MongodbSchema(
            String databaseName,
            String tableName,
            LinkedHashMap<String, DataType> fields,
            List<String> primaryKeys) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fields = fields;
        this.primaryKeys = primaryKeys;
    }

    public String tableName() {
        return tableName;
    }

    public String databaseName() {
        return databaseName;
    }

    public LinkedHashMap<String, DataType> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public static MongodbSchema getMongodbSchema(Configuration mongodbConfig) {
        SchemaAcquisitionMode mode = getModeFromConfig(mongodbConfig);
        switch (mode) {
            case SPECIFIED:
                return createSchemaFromSpecifiedConfig(mongodbConfig);
            case DYNAMIC:
                return createSchemaFromDynamicConfig(mongodbConfig);
            default:
                throw new IllegalArgumentException("Unsupported schema acquisition mode: " + mode);
        }
    }

    private static SchemaAcquisitionMode getModeFromConfig(Configuration mongodbConfig) {
        return SchemaAcquisitionMode.valueOf(mongodbConfig.get(START_MODE).toUpperCase());
    }

    private static MongodbSchema createSchemaFromSpecifiedConfig(Configuration mongodbConfig) {
        String[] columnNames = mongodbConfig.get(FIELD_NAME).split(",");
        LinkedHashMap<String, DataType> schemaFields =
                generateSchemaFields(Arrays.asList(columnNames));
        String databaseName = mongodbConfig.get(MongoDBSourceOptions.DATABASE);
        String collectionName = mongodbConfig.get(MongoDBSourceOptions.COLLECTION);
        return new MongodbSchema(
                databaseName, collectionName, schemaFields, Collections.singletonList(ID_FIELD));
    }

    private static MongodbSchema createSchemaFromDynamicConfig(Configuration mongodbConfig) {
        String hosts = mongodbConfig.get(MongoDBSourceOptions.HOSTS);
        String databaseName = mongodbConfig.get(MongoDBSourceOptions.DATABASE);
        String collectionName = mongodbConfig.get(MongoDBSourceOptions.COLLECTION);
        String url = String.format("mongodb://%s/%s", hosts, databaseName);
        try (MongoClient mongoClient = MongoClients.create(url)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            Document firstDocument = collection.find().first();
            return createMongodbSchema(databaseName, collectionName, getColumnNames(firstDocument));
        }
    }

    private static List<String> getColumnNames(Document document) {
        return document != null ? new ArrayList<>(document.keySet()) : Collections.emptyList();
    }

    private static LinkedHashMap<String, DataType> generateSchemaFields(List<String> columnNames) {
        LinkedHashMap<String, DataType> schemaFields = new LinkedHashMap<>();
        for (String columnName : columnNames) {
            schemaFields.put(columnName, DataTypes.STRING());
        }
        return schemaFields;
    }

    private static MongodbSchema createMongodbSchema(
            String databaseName, String collectionName, List<String> columnNames) {
        return new MongodbSchema(
                databaseName,
                collectionName,
                generateSchemaFields(columnNames),
                Collections.singletonList(ID_FIELD));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongodbSchema)) {
            return false;
        }
        MongodbSchema that = (MongodbSchema) o;
        return databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName)
                && fields.equals(that.fields)
                && primaryKeys.equals(that.primaryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, fields, primaryKeys);
    }
}
