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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTApi;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link ErrorResponse}. */
public class ErrorResponseTest {

    private static final Class<?>[] PRIMITIVE_CODE_CTOR = {
        String.class, String.class, String.class, int.class
    };

    @Test
    public void testPrimitiveCodeConstructorDescriptorIsRetained() {
        // The descriptor REST server implementations compiled against an earlier paimon-api
        // invoke. A source level new ErrorResponse(a, b, c, 404) would still compile if it were
        // deleted, because javac boxes into the Integer overload, so assert it reflectively.
        assertThatCode(() -> ErrorResponse.class.getConstructor(PRIMITIVE_CODE_CTOR))
                .doesNotThrowAnyException();
    }

    @Test
    public void testExactlyOneJsonCreatorAndItAcceptsNullableCode() throws Exception {
        List<Constructor<?>> creators =
                Arrays.stream(ErrorResponse.class.getDeclaredConstructors())
                        .filter(c -> c.isAnnotationPresent(JsonCreator.class))
                        .collect(Collectors.toList());

        assertThat(creators).hasSize(1);
        assertThat(creators.get(0).getParameterTypes())
                .containsExactly(String.class, String.class, String.class, Integer.class);
        // the primitive overload must stay invisible to Jackson, otherwise an absent code
        // deserializes to 0 again
        assertThat(
                        ErrorResponse.class
                                .getConstructor(PRIMITIVE_CODE_CTOR)
                                .isAnnotationPresent(JsonCreator.class))
                .isFalse();
    }

    @Test
    public void testCodeIsAbsentOnTheWireRatherThanZero() throws Exception {
        assertThat(RESTApi.fromJson("{\"message\":\"x\"}", ErrorResponse.class).getCode()).isNull();
        assertThat(
                        RESTApi.fromJson("{\"message\":\"x\",\"code\":null}", ErrorResponse.class)
                                .getCode())
                .isNull();
        assertThat(
                        RESTApi.fromJson("{\"message\":\"x\",\"code\":404}", ErrorResponse.class)
                                .getCode())
                .isEqualTo(404);
    }

    @Test
    public void testBothConstructorsAgree() throws Exception {
        assertThat(new ErrorResponse("TABLE", "t", "m", 404).getCode()).isEqualTo(404);
        assertThat(new ErrorResponse("TABLE", "t", "m", (Integer) null).getCode()).isNull();
        assertThat(RESTApi.toJson(new ErrorResponse(null, null, "m", (Integer) null)))
                .contains("\"code\":null");
    }
}
