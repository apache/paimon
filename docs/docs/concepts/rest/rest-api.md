---
title: "REST API"
hide_table_of_contents: true
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

The OpenAPI 3.1 document below defines the language-neutral wire contract for REST Catalog
servers and clients. It can also be used to generate or validate SDK models in other languages.

Partition options use the existing `POST .../partitions` request. `partitionOptions` follows the
order of `partitionSpecs`; use `{}` when a partition has no options. Custom locations use the
`path` option. Before registering custom locations, ensure that the REST server supports partition
options and all readers support custom locations.

<body>
    <iframe src="/docs/master/rest-catalog-open-api.yaml" width="100%" height="800px" />
</body>
