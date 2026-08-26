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

package org.apache.paimon.rest.requests;

import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests request DTOs with Paimon's shaded and external Jackson mappers. */
public class RequestJacksonCompatibilityTest {

    private static final com.fasterxml.jackson.databind.ObjectMapper EXTERNAL_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static final List<RequestCase<?>> SIMPLE_REQUESTS =
            Arrays.asList(
                    requestCase(
                            AlterDatabaseRequest.class,
                            "{\"removals\":[\"owner\"],\"updates\":{\"comment\":\"analytics\"}}",
                            request -> {
                                assertThat(request.getRemovals()).containsExactly("owner");
                                assertThat(request.getUpdates())
                                        .containsEntry("comment", "analytics");
                            },
                            "removals",
                            "updates"),
                    requestCase(
                            AuthTableQueryRequest.class,
                            "{\"select\":[\"id\",\"name\"]}",
                            request -> assertThat(request.select()).containsExactly("id", "name"),
                            "select"),
                    requestCase(
                            CreateBranchRequest.class,
                            "{\"branch\":\"audit\",\"fromTag\":\"v1\"}",
                            request -> {
                                assertThat(request.branch()).isEqualTo("audit");
                                assertThat(request.fromTag()).isEqualTo("v1");
                            },
                            "branch",
                            "fromTag"),
                    requestCase(
                            CreateDatabaseRequest.class,
                            "{\"name\":\"warehouse\",\"options\":{\"owner\":\"alice\"}}",
                            request -> {
                                assertThat(request.getName()).isEqualTo("warehouse");
                                assertThat(request.getOptions()).containsEntry("owner", "alice");
                            },
                            "name",
                            "options"),
                    requestCase(
                            CreateTagRequest.class,
                            "{\"tagName\":\"v2\",\"snapshotId\":42,\"timeRetained\":\"7 d\"}",
                            request -> {
                                assertThat(request.tagName()).isEqualTo("v2");
                                assertThat(request.snapshotId()).isEqualTo(42L);
                                assertThat(request.timeRetained()).isEqualTo("7 d");
                            },
                            "tagName",
                            "snapshotId",
                            "timeRetained"),
                    requestCase(
                            DropPartitionsRequest.class,
                            "{\"partitionSpecs\":[{\"dt\":\"2026-08-24\"}],"
                                    + "\"ignoreIfNotExists\":false}",
                            request -> {
                                assertThat(request.getPartitionSpecs())
                                        .containsExactly(
                                                Collections.singletonMap("dt", "2026-08-24"));
                                assertThat(request.ignoreIfNotExists()).isFalse();
                            },
                            "partitionSpecs",
                            "ignoreIfNotExists"),
                    requestCase(
                            ListPartitionsByFilterRequest.class,
                            "{\"filter\":\"dt = '2026-08-24'\","
                                    + "\"partitionNamePattern\":\"dt=*\","
                                    + "\"maxResults\":25,\"pageToken\":\"next\"}",
                            request -> {
                                assertThat(request.getFilter()).isEqualTo("dt = '2026-08-24'");
                                assertThat(request.getPartitionNamePattern()).isEqualTo("dt=*");
                                assertThat(request.getMaxResults()).isEqualTo(25);
                                assertThat(request.getPageToken()).isEqualTo("next");
                            },
                            "filter",
                            "partitionNamePattern",
                            "maxResults",
                            "pageToken"),
                    requestCase(
                            ListPartitionsByNamesRequest.class,
                            "{\"specs\":[{\"dt\":\"2026-08-24\"}]}",
                            request ->
                                    assertThat(request.getPartitionSpecs())
                                            .containsExactly(
                                                    Collections.singletonMap("dt", "2026-08-24")),
                            "specs"),
                    requestCase(
                            MarkDonePartitionsRequest.class,
                            "{\"specs\":[{\"dt\":\"2026-08-24\"}]}",
                            request ->
                                    assertThat(request.getPartitionSpecs())
                                            .containsExactly(
                                                    Collections.singletonMap("dt", "2026-08-24")),
                            "specs"),
                    requestCase(
                            ResetConsumerRequest.class,
                            "{\"consumerId\":\"etl\",\"nextSnapshotId\":43}",
                            request -> {
                                assertThat(request.consumerId()).isEqualTo("etl");
                                assertThat(request.nextSnapshotId()).isEqualTo(43L);
                            },
                            "consumerId",
                            "nextSnapshotId"),
                    requestCase(
                            RollbackSchemaRequest.class,
                            "{\"schemaId\":44}",
                            request -> assertThat(request.getSchemaId()).isEqualTo(44L),
                            "schemaId"));

    private static final Set<Class<? extends RESTRequest>> COMPLEX_REQUESTS =
            Stream.<Class<? extends RESTRequest>>of(
                            AlterFunctionRequest.class,
                            AlterTableRequest.class,
                            AlterViewRequest.class,
                            CommitTableRequest.class,
                            CreateFunctionRequest.class,
                            CreatePartitionsRequest.class,
                            CreateTableRequest.class,
                            CreateViewRequest.class,
                            GrantPermissionRequest.class,
                            RegisterTableRequest.class,
                            RenameTableRequest.class,
                            ReplaceTableRequest.class,
                            RevokePermissionRequest.class,
                            RollbackTableRequest.class)
                    .collect(Collectors.toSet());

    @ParameterizedTest(name = "{0}")
    @MethodSource("simpleRequests")
    void testExternalJacksonDeserializesEveryField(RequestCase<?> requestCase) throws Exception {
        requestCase.assertFields(requestCase.read(EXTERNAL_MAPPER));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("simpleRequests")
    void testShadedJacksonRoundTrips(RequestCase<?> requestCase) throws Exception {
        RESTRequest request = requestCase.read(EXTERNAL_MAPPER);
        requestCase.assertFields(
                RESTApi.OBJECT_MAPPER.readValue(
                        RESTApi.OBJECT_MAPPER.writeValueAsString(request), requestCase.type));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("simpleRequests")
    void testConstructorPropertyNamesAndOrder(RequestCase<?> requestCase) {
        Constructor<?> creator = jsonCreator(requestCase.type);

        assertThat(creator.getAnnotation(ConstructorProperties.class))
                .isNotNull()
                .extracting(ConstructorProperties::value)
                .isEqualTo(requestCase.propertyNames);
    }

    @Test
    void testRequestCreatorAllowlistsAreComplete() throws Exception {
        Set<Class<? extends RESTRequest>> simpleRequests =
                SIMPLE_REQUESTS.stream()
                        .map(requestCase -> requestCase.type)
                        .collect(Collectors.toSet());
        Set<Class<? extends RESTRequest>> expected = new LinkedHashSet<>(simpleRequests);
        expected.addAll(COMPLEX_REQUESTS);

        assertThat(findConcreteRequestCreators()).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(simpleRequests)
                .allSatisfy(
                        type ->
                                assertThat(jsonCreator(type).getGenericParameterTypes())
                                        .allMatch(
                                                RequestJacksonCompatibilityTest
                                                        ::isOrdinaryJacksonType));
        assertThat(COMPLEX_REQUESTS)
                .allSatisfy(
                        type ->
                                assertThat(jsonCreator(type).getGenericParameterTypes())
                                        .anyMatch(parameter -> !isOrdinaryJacksonType(parameter)));
    }

    @Test
    void testUnknownPropertiesFollowExternalMapperConfiguration() throws Exception {
        String json = "{\"name\":\"warehouse\",\"options\":{},\"unknown\":true}";

        assertThatThrownBy(() -> EXTERNAL_MAPPER.readValue(json, CreateDatabaseRequest.class))
                .isInstanceOf(
                        com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException.class);

        com.fasterxml.jackson.databind.ObjectMapper lenientMapper =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .configure(
                                com.fasterxml.jackson.databind.DeserializationFeature
                                        .FAIL_ON_UNKNOWN_PROPERTIES,
                                false);
        assertThat(lenientMapper.readValue(json, CreateDatabaseRequest.class).getName())
                .isEqualTo("warehouse");
    }

    private static Stream<RequestCase<?>> simpleRequests() {
        return SIMPLE_REQUESTS.stream();
    }

    private static Constructor<?> jsonCreator(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
                .filter(constructor -> constructor.isAnnotationPresent(JsonCreator.class))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No @JsonCreator constructor on " + type));
    }

    private static Set<Class<? extends RESTRequest>> findConcreteRequestCreators()
            throws Exception {
        String packageName = RequestJacksonCompatibilityTest.class.getPackage().getName();
        Path packagePath =
                Paths.get(
                                RESTRequest.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())
                        .resolve(packageName.replace('.', '/'));

        try (Stream<Path> paths = Files.list(packagePath)) {
            Set<Class<? extends RESTRequest>> requests = new LinkedHashSet<>();
            for (Path path :
                    paths.filter(file -> file.getFileName().toString().endsWith("Request.class"))
                            .collect(Collectors.toList())) {
                String className =
                        packageName
                                + "."
                                + path.getFileName().toString().replaceFirst("\\.class$", "");
                Class<?> type = Class.forName(className);
                if (RESTRequest.class.isAssignableFrom(type)
                        && !Modifier.isAbstract(type.getModifiers())
                        && Arrays.stream(type.getDeclaredConstructors())
                                .anyMatch(
                                        constructor ->
                                                constructor.isAnnotationPresent(
                                                        JsonCreator.class))) {
                    requests.add(type.asSubclass(RESTRequest.class));
                }
            }
            return requests;
        }
    }

    private static boolean isOrdinaryJacksonType(Type type) {
        if (type instanceof Class<?>) {
            Class<?> clazz = (Class<?>) type;
            return clazz.isPrimitive()
                    || clazz == String.class
                    || clazz == Boolean.class
                    || clazz == Character.class
                    || Number.class.isAssignableFrom(clazz);
        }
        if (!(type instanceof ParameterizedType)) {
            return false;
        }

        ParameterizedType parameterizedType = (ParameterizedType) type;
        Class<?> rawType = (Class<?>) parameterizedType.getRawType();
        Type[] arguments = parameterizedType.getActualTypeArguments();
        if (Collection.class.isAssignableFrom(rawType)) {
            return arguments.length == 1 && isOrdinaryJacksonType(arguments[0]);
        }
        return Map.class.isAssignableFrom(rawType)
                && arguments.length == 2
                && arguments[0] == String.class
                && isOrdinaryJacksonType(arguments[1]);
    }

    private static <T extends RESTRequest> RequestCase<T> requestCase(
            Class<T> type, String json, Consumer<T> assertions, String... propertyNames) {
        return new RequestCase<>(type, json, assertions, propertyNames);
    }

    private static class RequestCase<T extends RESTRequest> {

        private final Class<T> type;
        private final String json;
        private final Consumer<T> assertions;
        private final String[] propertyNames;

        private RequestCase(
                Class<T> type, String json, Consumer<T> assertions, String[] propertyNames) {
            this.type = type;
            this.json = json;
            this.assertions = assertions;
            this.propertyNames = propertyNames;
        }

        private T read(com.fasterxml.jackson.databind.ObjectMapper mapper) throws Exception {
            return mapper.readValue(json, type);
        }

        private void assertFields(RESTRequest request) {
            assertions.accept(type.cast(request));
        }

        @Override
        public String toString() {
            return type.getSimpleName();
        }
    }
}
