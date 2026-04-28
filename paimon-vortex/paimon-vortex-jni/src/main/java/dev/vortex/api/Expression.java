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

package dev.vortex.api;

import dev.vortex.api.expressions.*;
import java.util.List;
import java.util.Optional;

/** Vortex expression language. */
public interface Expression {
    String id();

    List<Expression> children();

    Optional<byte[]> metadata();

    default <T> T accept(Visitor<T> visitor) {
        return visitor.visitOther(this);
    }

    interface Visitor<T> {
        T visitLiteral(Literal<?> literal);

        T visitRoot(Root root);

        T visitBinary(Binary binary);

        T visitNot(Not not);

        T visitGetItem(GetItem getItem);

        T visitIsNull(IsNull isNull);

        T visitIsNotNull(IsNotNull isNotNull);

        T visitOther(Expression expression);
    }
}
