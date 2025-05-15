---
title: "REST API"
weight: 1
type: docs
aliases:
- /api/rest-api.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# REST API

This is Java API for [REST]({{< ref "concepts/rest/overview" >}}).

## Dependency

Maven dependency:

```xml
<dependency>
  <groupId>org.apache.paimon</groupId>
  <artifactId>paimon-api</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

Or download the jar file:
{{< stable >}}[Paimon API](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-api/{{< version >}}/paimon-api-{{< version >}}.jar).{{< /stable >}}
{{< unstable >}}[Paimon API](https://repository.apache.org/snapshots/org/apache/paimon/paimon-api/{{< version >}}/).{{< /unstable >}}

## RESTApi

```java
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;

import java.util.List;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;

public class RESTApiExample {

    public static void main(String[] args) {
        Options options = new Options();
        options.set(URI, "<catalog server url>");
        options.set(WAREHOUSE, "my_instance_name");
        options.set(TOKEN_PROVIDER, "dlf");
        options.set(DLF_ACCESS_KEY_ID, "<access-key-id>");
        options.set(DLF_ACCESS_KEY_SECRET, "<access-key-secret>");

        RESTApi api = new RESTApi(options);
        List<String> tables = api.listTables("my_database");
        System.out.println(tables);
    }
}
```

See more methods in `'RESTApi'`.
