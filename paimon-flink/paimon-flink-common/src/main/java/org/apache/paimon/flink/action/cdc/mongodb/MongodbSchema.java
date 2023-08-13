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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;
import static org.apache.paimon.shade.guava30.com.google.common.collect.Lists.newArrayList;
import static org.apache.paimon.types.DataTypes.STRING;

/** mongodb schema. */
public class MongodbSchema {

    private final String databaseName;
    private final String tableName;
    private final Map<String, DataType> fields;
    private final List<String> primaryKeys;

    public MongodbSchema(
            String databaseName,
            String tableName,
            Map<String, DataType> fields,
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

    public Map<String, DataType> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public static MongodbSchema getMongodbSchema(Configuration mongodbConfig) {

        ModeEnum mode = ModeEnum.valueOf(mongodbConfig.get(START_MODE).toUpperCase());
        String hosts = mongodbConfig.get(MongoDBSourceOptions.HOSTS);
        String databaseName = mongodbConfig.get(MongoDBSourceOptions.DATABASE);
        String collectionName = mongodbConfig.get(MongoDBSourceOptions.COLLECTION);
        switch (mode) {
            case SPECIFIED:
                String[] columnNames = mongodbConfig.get(FIELD_NAME).split(",");
                Map<String, DataType> schemaFields =
                        generateSchemaFields(Arrays.asList(columnNames));
                return new MongodbSchema(
                        databaseName, collectionName, schemaFields, newArrayList("_id"));
            case DYNAMIC:
                String url = String.format("mongodb://%s/%s", hosts, databaseName);
                try (MongoClient mongoClient = MongoClients.create(url)) {
                    MongoDatabase database = mongoClient.getDatabase(databaseName);
                    MongoCollection<Document> collection = database.getCollection(collectionName);
                    Document firstDocument = collection.find().first();
                    return createMongodbSchema(
                            databaseName, collectionName, getColumnNames(firstDocument));
                }
            default:
                throw new RuntimeException();
        }
    }

    private static List<String> getColumnNames(Document document) {
        if (document != null) {
            return new ArrayList<>(document.keySet());
        }
        return null;
    }

    private static Map<String, DataType> generateSchemaFields(List<String> columnNames) {
        Map<String, DataType> schemaFields = new LinkedHashMap<>();

        if (columnNames != null) {
            for (String columnName : columnNames) {
                schemaFields.put(columnName, STRING());
            }
        }

        return schemaFields;
    }

    private static MongodbSchema createMongodbSchema(
            String databaseName, String collectionName, List<String> columnNames) {
        Map<String, DataType> schemaFields = generateSchemaFields(columnNames);
        return new MongodbSchema(databaseName, collectionName, schemaFields, newArrayList("_id"));
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
