<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Open API spec

The `rest-catalog-open-api.yaml` defines the REST catalog interface.

## Generate Open API Spec
```sh
mvn spring-boot:run &
curl -s "http://localhost:8080/swagger-api-docs" | jq -M > /Users/jerry/code/paimon/yt-paimon/paimon/paimon-open-api/rest-catalog-open-api.json
yq --prettyPrint -o=yaml /Users/jerry/code/paimon/yt-paimon/paimon/paimon-open-api/rest-catalog-open-api.json > /Users/jerry/code/paimon/yt-paimon/paimon/paimon-open-api/rest-catalog-open-api.yaml
rm -rf /Users/jerry/code/paimon/yt-paimon/paimon/paimon-open-api/rest-catalog-open-api.json
```