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

package org.apache.paimon.open.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/** Config for OpenAPI. */
@Configuration
public class OpenAPIConfig {

    @Value("${openapi.url}")
    private String devUrl;

    @Bean
    public OpenAPI restCatalogOpenAPI() {
        Server server = new Server();
        server.setUrl(devUrl);
        server.setDescription("Server URL in Development environment");

        License mitLicense =
                new License()
                        .name("Apache 2.0")
                        .url("https://www.apache.org/licenses/LICENSE-2.0.html");

        Info info =
                new Info()
                        .title("RESTCatalog API")
                        .version("1.0")
                        .description("This API exposes endpoints to RESTCatalog.")
                        .license(mitLicense);
        List<Server> servers = new ArrayList<>();
        servers.add(server);
        return new OpenAPI().info(info).servers(servers);
    }
}
