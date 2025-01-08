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
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.exceptions.ServiceFailureException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.exceptions.UnsupportedOperationException;
import org.apache.paimon.rest.responses.ErrorResponse;

/** Default error handler. */
public class DefaultErrorHandler extends ErrorHandler {

    private static final ErrorHandler INSTANCE = new DefaultErrorHandler();

    public static ErrorHandler getInstance() {
        return INSTANCE;
    }

    @Override
    public void accept(ErrorResponse error) {
        int code = error.getCode();
        String message = error.getMessage();
        switch (code) {
            case 400:
                throw new BadRequestException(String.format("%s", message));
            case 401:
                throw new NotAuthorizedException("Not authorized: %s", message);
            case 403:
                throw new ForbiddenException("Forbidden: %s", message);
            case 404:
                throw new NoSuchResourceException(
                        error.getResourceType(), error.getResourceName(), "%s", message);
            case 405:
            case 406:
                break;
            case 409:
                throw new AlreadyExistsException(
                        error.getResourceType(), error.getResourceName(), "%s", message);
            case 500:
                throw new ServiceFailureException("Server error: %s", message);
            case 501:
                throw new UnsupportedOperationException(message);
            case 503:
                throw new ServiceUnavailableException("Service unavailable: %s", message);
            default:
                break;
        }

        throw new RESTException("Unable to process: %s", message);
    }
}
