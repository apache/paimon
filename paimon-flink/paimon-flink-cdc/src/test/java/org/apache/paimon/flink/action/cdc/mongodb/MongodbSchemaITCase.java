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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link MongodbSchemaUtils}. */
public class MongodbSchemaITCase extends MongoDBActionITCaseBase {

    @BeforeAll
    public static void initMongoDB() {
        // Create a real MongoDB client and insert a document to infer the schema
        MongoClientSettings.Builder settingsBuilder =
                MongoClientSettings.builder()
                        .applyToClusterSettings(
                                builder ->
                                        builder.hosts(
                                                Collections.singletonList(
                                                        new ServerAddress(
                                                                MONGODB_CONTAINER
                                                                        .getHostAndPort()))))
                        .credential(
                                MongoCredential.createCredential(
                                        MongoDBContainer.PAIMON_USER,
                                        "admin",
                                        MongoDBContainer.PAIMON_USER_PASSWORD.toCharArray()));

        MongoClientSettings settings = settingsBuilder.build();
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase database = mongoClient.getDatabase("testDatabase");
            MongoCollection<Document> collection = database.getCollection("testCollection");
            Document doc = new Document("name", "Alice").append("age", 30);
            collection.insertOne(doc);
        }
    }

    @Test
    public void testCreateSchemaFromValidConfig() {
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.setString(MongoDBSourceOptions.HOSTS, MONGODB_CONTAINER.getHostAndPort());
        mongodbConfig.setString(MongoDBSourceOptions.USERNAME, MongoDBContainer.PAIMON_USER);
        mongodbConfig.setString(
                MongoDBSourceOptions.PASSWORD, MongoDBContainer.PAIMON_USER_PASSWORD);
        mongodbConfig.setString(MongoDBSourceOptions.CONNECTION_OPTIONS, "authSource=admin");
        mongodbConfig.setString(MongoDBSourceOptions.DATABASE, "testDatabase");
        mongodbConfig.setString(MongoDBSourceOptions.COLLECTION, "testCollection");
        Schema schema = MongodbSchemaUtils.getMongodbSchema(mongodbConfig);
        assertNotNull(schema);
    }

    @Test
    public void testCreateSchemaFromInvalidHost() {
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.setString(MongoDBSourceOptions.HOSTS, "127.0.0.1:12345");
        mongodbConfig.setString(MongoDBSourceOptions.USERNAME, MongoDBContainer.PAIMON_USER);
        mongodbConfig.setString(
                MongoDBSourceOptions.PASSWORD, MongoDBContainer.PAIMON_USER_PASSWORD);
        mongodbConfig.setString(MongoDBSourceOptions.CONNECTION_OPTIONS, "authSource=admin");
        mongodbConfig.setString(MongoDBSourceOptions.DATABASE, "testDatabase");
        mongodbConfig.setString(MongoDBSourceOptions.COLLECTION, "testCollection");

        assertThrows(
                RuntimeException.class, () -> MongodbSchemaUtils.getMongodbSchema(mongodbConfig));
    }

    @Test
    public void testCreateSchemaFromIncompleteConfig() {
        // Create a Configuration object with missing necessary settings
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.setString(MongoDBSourceOptions.HOSTS, MONGODB_CONTAINER.getHostAndPort());
        // Expect an exception to be thrown due to missing necessary settings
        assertThrows(
                NullPointerException.class,
                () -> MongodbSchemaUtils.getMongodbSchema(mongodbConfig));
    }

    @Test
    public void testCreateSchemaFromDynamicConfig() {
        // Create a Configuration object with the necessary settings
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.setString(MongoDBSourceOptions.HOSTS, MONGODB_CONTAINER.getHostAndPort());
        mongodbConfig.setString(MongoDBSourceOptions.USERNAME, MongoDBContainer.PAIMON_USER);
        mongodbConfig.setString(
                MongoDBSourceOptions.PASSWORD, MongoDBContainer.PAIMON_USER_PASSWORD);
        mongodbConfig.setString(MongoDBSourceOptions.CONNECTION_OPTIONS, "authSource=admin");
        mongodbConfig.setString(MongoDBSourceOptions.DATABASE, "testDatabase");
        mongodbConfig.setString(MongoDBSourceOptions.COLLECTION, "testCollection");

        // Call the method and check the results
        Schema schema = MongodbSchemaUtils.getMongodbSchema(mongodbConfig);

        // Verify the schema
        assertNotNull(schema);

        List<DataField> expectedFields = new ArrayList<>();
        expectedFields.add(new DataField(0, "_id", DataTypes.STRING().notNull()));
        expectedFields.add(new DataField(1, "name", DataTypes.STRING()));
        expectedFields.add(new DataField(2, "age", DataTypes.STRING()));

        assertEquals(expectedFields, schema.fields());
    }

    @Test
    public void testCreateSchemaFromInvalidDatabase() {
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.setString(MongoDBSourceOptions.HOSTS, MONGODB_CONTAINER.getHostAndPort());
        mongodbConfig.setString(MongoDBSourceOptions.USERNAME, MongoDBContainer.PAIMON_USER);
        mongodbConfig.setString(
                MongoDBSourceOptions.PASSWORD, MongoDBContainer.PAIMON_USER_PASSWORD);
        mongodbConfig.setString(MongoDBSourceOptions.CONNECTION_OPTIONS, "authSource=admin");
        mongodbConfig.setString(MongoDBSourceOptions.DATABASE, "invalidDatabase");
        mongodbConfig.setString(MongoDBSourceOptions.COLLECTION, "testCollection");

        assertThrows(
                RuntimeException.class, () -> MongodbSchemaUtils.getMongodbSchema(mongodbConfig));
    }

    @Test
    public void testCreateSchemaFromInvalidCollection() {
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.setString(MongoDBSourceOptions.HOSTS, MONGODB_CONTAINER.getHostAndPort());
        mongodbConfig.setString(MongoDBSourceOptions.USERNAME, MongoDBContainer.PAIMON_USER);
        mongodbConfig.setString(
                MongoDBSourceOptions.PASSWORD, MongoDBContainer.PAIMON_USER_PASSWORD);
        mongodbConfig.setString(MongoDBSourceOptions.CONNECTION_OPTIONS, "authSource=admin");
        mongodbConfig.setString(MongoDBSourceOptions.DATABASE, "testDatabase");
        mongodbConfig.setString(MongoDBSourceOptions.COLLECTION, "invalidCollection");

        assertThrows(
                RuntimeException.class, () -> MongodbSchemaUtils.getMongodbSchema(mongodbConfig));
    }
}
