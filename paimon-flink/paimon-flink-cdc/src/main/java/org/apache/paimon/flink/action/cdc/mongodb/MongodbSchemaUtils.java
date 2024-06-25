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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.encodeValue;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;

/**
 * Build schema from a MongoDB collection.
 *
 * <p>The schema can be acquired in two modes: SPECIFIED and DYNAMIC. In the SPECIFIED mode, the
 * schema details are provided explicitly, while in the DYNAMIC mode, the schema is inferred from
 * the first document in the collection.
 */
public class MongodbSchemaUtils {

    private static final String ID_FIELD = "_id";

    /**
     * The schema can be created in one of the two modes:
     *
     * <ul>
     *   <li><b>SPECIFIED</b>: In this mode, the schema is created based on the explicit column
     *       names provided in the configuration. The data types for all columns are assumed to be
     *       STRING.
     *   <li><b>DYNAMIC</b>: In this mode, the schema is inferred dynamically from the first
     *       document in the specified MongoDB collection.
     * </ul>
     *
     * <p>The Configuration object passed to the createSchema method should have the necessary
     * MongoDB configuration properties set, including the host address, database name, collection
     * name, and optionally, the username and password for authentication. For the SPECIFIED mode,
     * the field names should also be specified in the configuration.
     */
    public static Schema getMongodbSchema(Configuration mongodbConfig) {
        SchemaAcquisitionMode mode = getModeFromConfig(mongodbConfig);
        String databaseName =
                Objects.requireNonNull(
                        mongodbConfig.get(MongoDBSourceOptions.DATABASE),
                        "Database name cannot be null");
        String collectionName =
                Objects.requireNonNull(
                        mongodbConfig.get(MongoDBSourceOptions.COLLECTION),
                        "Collection name cannot be null");

        switch (mode) {
            case SPECIFIED:
                String[] columnNames =
                        Objects.requireNonNull(
                                        mongodbConfig.get(FIELD_NAME), "Field names cannot be null")
                                .split(",");

                return createMongodbSchema(columnNames);
            case DYNAMIC:
                String hosts =
                        Objects.requireNonNull(
                                mongodbConfig.get(MongoDBSourceOptions.HOSTS),
                                "Hosts cannot be null");

                MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

                settingsBuilder.applyConnectionString(
                        new ConnectionString(
                                buildConnectionString(
                                        mongodbConfig.get(MongoDBSourceOptions.USERNAME),
                                        mongodbConfig.get(MongoDBSourceOptions.PASSWORD),
                                        mongodbConfig.get(MongoDBSourceOptions.SCHEME),
                                        hosts,
                                        mongodbConfig.get(
                                                MongoDBSourceOptions.CONNECTION_OPTIONS))));

                MongoClientSettings settings = settingsBuilder.build();

                try (MongoClient mongoClient = MongoClients.create(settings)) {
                    MongoDatabase database = mongoClient.getDatabase(databaseName);
                    MongoCollection<Document> collection = database.getCollection(collectionName);
                    Document firstDocument = collection.find().first();

                    if (firstDocument == null) {
                        throw new IllegalStateException(
                                "No documents in collection to infer schema");
                    }

                    return createMongodbSchema(getColumnNames(firstDocument));
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to create schema from MongoDB collection", e);
                }
            default:
                throw new IllegalArgumentException("Unsupported schema acquisition mode: " + mode);
        }
    }

    private static String buildConnectionString(
            @Nullable String username,
            @Nullable String password,
            String scheme,
            String hosts,
            @Nullable String connectionOptions) {
        StringBuilder sb = new StringBuilder(scheme).append("://");

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            sb.append(encodeValue(username)).append(":").append(encodeValue(password)).append("@");
        }

        sb.append(checkNotNull(hosts));

        if (StringUtils.isNotEmpty(connectionOptions)) {
            sb.append("/?").append(connectionOptions);
        }

        return sb.toString();
    }

    private static SchemaAcquisitionMode getModeFromConfig(Configuration mongodbConfig) {
        return SchemaAcquisitionMode.valueOf(mongodbConfig.get(START_MODE).toUpperCase());
    }

    private static List<String> getColumnNames(Document document) {
        return document != null ? new ArrayList<>(document.keySet()) : Collections.emptyList();
    }

    private static Schema createMongodbSchema(String[] columnNames) {
        return createMongodbSchema(Arrays.asList(columnNames));
    }

    private static Schema createMongodbSchema(List<String> columnNames) {
        Schema.Builder builder = Schema.newBuilder();
        for (String column : columnNames) {
            builder.column(column, DataTypes.STRING());
        }

        builder.primaryKey(ID_FIELD);

        return builder.build();
    }
}
