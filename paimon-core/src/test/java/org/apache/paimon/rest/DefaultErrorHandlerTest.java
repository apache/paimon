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

package org.apache.paimon.rest;

import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.exceptions.ServiceFailureException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.responses.ErrorResponse;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.paimon.rest.interceptor.LoggingInterceptor.DEFAULT_REQUEST_ID;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link DefaultErrorHandler}. */
public class DefaultErrorHandlerTest {
    private ErrorHandler defaultErrorHandler;

    @Before
    public void setUp() throws IOException {
        defaultErrorHandler = DefaultErrorHandler.getInstance();
    }

    @Test
    public void testHandleErrorResponse() {
        assertThrows(
                BadRequestException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(400), DEFAULT_REQUEST_ID));
        assertThrows(
                NotAuthorizedException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(401), DEFAULT_REQUEST_ID));
        assertThrows(
                ForbiddenException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(403), DEFAULT_REQUEST_ID));
        assertThrows(
                NoSuchResourceException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(404), DEFAULT_REQUEST_ID));
        assertThrows(
                RESTException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(405), DEFAULT_REQUEST_ID));
        assertThrows(
                RESTException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(406), DEFAULT_REQUEST_ID));
        assertThrows(
                AlreadyExistsException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(409), DEFAULT_REQUEST_ID));
        assertThrows(
                ServiceFailureException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(500), DEFAULT_REQUEST_ID));
        assertThrows(
                NotImplementedException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(501), DEFAULT_REQUEST_ID));
        assertThrows(
                RESTException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(502), DEFAULT_REQUEST_ID));
        assertThrows(
                ServiceUnavailableException.class,
                () -> defaultErrorHandler.accept(generateErrorResponse(503), DEFAULT_REQUEST_ID));
    }

    private ErrorResponse generateErrorResponse(int code) {
        return new ErrorResponse(null, null, "message", code);
    }
}
