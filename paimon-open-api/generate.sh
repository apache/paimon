#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start the application
cd ..
mvn clean install -DskipTests
cd ./paimon-open-api
mvn spring-boot:run &
SPRING_PID=$!
# Wait for the application to be ready
RETRY_COUNT=0
MAX_RETRIES=10
SLEEP_DURATION=5

until $(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/swagger-api-docs | grep -q "200"); do
    ((RETRY_COUNT++))
    if [ $RETRY_COUNT -gt $MAX_RETRIES ]; then
        echo "Failed to start the application after $MAX_RETRIES retries."
        exit 1
    fi
    echo "Application not ready yet. Retrying in $SLEEP_DURATION seconds..."
    sleep $SLEEP_DURATION
done

echo "Application is ready".

# Generate the OpenAPI specification file
curl -s "http://localhost:8080/swagger-api-docs" | jq -M > ./rest-catalog-open-api.json
yq --prettyPrint -o=yaml ./rest-catalog-open-api.json > ./rest-catalog-open-api.yaml
rm -rf ./rest-catalog-open-api.json
mvn spotless:apply
# Stop the application
echo "Stopping application..."
kill $SPRING_PID