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

# Apache Paimon Security Threat Model

This document describes Apache Paimon's detailed security threat model for
maintainers and automated security triage.

It complements the shorter public-facing security model in
[`docs/docs/project/security.md`](docs/docs/project/security.md) (published at the project website) by making
Paimon's trust assumptions, security boundaries, and recurring non-security
bug classes more explicit.

## Purpose

Apache Paimon is a streaming data lake platform that is often deployed as a
library and integration layer inside larger systems (Flink, Spark, Hive, and
other query engines) that provide their own authentication, authorization, and
credential management. Because of that deployment model, many bug classes that
look security-relevant in the abstract are not actually security
vulnerabilities in Paimon itself.

This model is intended to answer:

- what Paimon generally treats as a security vulnerability
- what Paimon generally treats as correctness, hardening, or deployment work
- which boundaries are primarily owned by Paimon versus the surrounding
  catalog, engine, or service
- which issue classes should be downgraded by default by scanners

## Scope

This model is scoped to the Apache Paimon project itself:

- the table format implementation (paimon-core)
- client libraries (paimon-api, paimon-common)
- the REST Catalog client and protocol (paimon-api, paimon-core)
- engine integrations (Flink, Spark, Hive connectors)
- the Python client (pypaimon)

It is not a general threat model for every deployment that embeds Paimon.

In particular, it does not attempt to define the complete security model for:

- query engines or applications that embed Paimon
- storage-level authorization enforced outside Paimon
- REST Catalog server implementations (Paimon defines the client and protocol,
  not the server)

## Security Goals

Paimon should:

- avoid exposing secrets or delegated credentials to principals that were not
  already trusted with them
- avoid creating new unauthorized capabilities in Paimon-owned components or
  integrations
- avoid violating trust boundaries that Paimon itself owns, such as leaking
  auth, signer, or credential-bearing state across catalog or session
  boundaries in the same process
- avoid leaking delegated storage tokens (data tokens) across table or
  principal boundaries

Paimon does not aim to be the primary enforcement point for:

- user-to-user authorization inside a query engine
- storage-level authorization (e.g., object store IAM policies)
- service-side authorization performed by a REST Catalog server
- row-level or column-level access control (Paimon relays server-provided
  filters and column masking rules, but enforcement is in the server)

## Roles

### Operator

The operator deploys and configures the catalog, REST Catalog server, engine,
and storage integration around Paimon. This role is trusted to choose
endpoints, warehouses, and storage integrations, configure credentials, and 
decide which users may create, read, or modify tables.

### Catalog Control Plane

The catalog control plane is responsible for resolving tables and supplying
metadata, locations, configuration, and delegated credentials to Paimon.
This role may be implemented by:

- a REST Catalog server
- a Hive Metastore
- a JDBC-backed catalog
- a filesystem-based catalog

Regardless of implementation, it should not expose secrets to unintended
principals or leak credential-bearing state across unintended boundaries.

Paimon assumes a trusted catalog or metastore, which is outside its primary
security boundary.

### REST Catalog Server

In REST deployments, part of the catalog control plane is implemented by a
server that returns metadata, configuration, delegated storage credentials
(data tokens), and query-level authorization (row filters and column masking)
to the client. This server is generally treated as a trusted control-plane
component.

The REST Catalog server is responsible for:

- authenticating clients
- authorizing catalog operations (create/drop/alter databases, tables, views,
  functions)
- issuing scoped, time-limited data tokens for storage access
- providing row-level filters and column masking rules via the auth table
  query API
- returning server-side configuration to merge with client configuration

### REST Catalog Client

In REST deployments, the client-side catalog (`RESTCatalog`, `RESTApi`)
consumes server-provided metadata, configuration, and credentials. Where the
client and server are meaningfully distinct, client-side bugs in token
handling, caching, or reuse may still be security-relevant. This is especially
true when the Paimon-owned client implementation leaks credential-bearing
state across catalog, session, or principal boundaries it is expected to
preserve.

The REST Catalog client is responsible for:

- sending authenticated requests using a configured `AuthProvider`
- refreshing tokens before expiration (with a configurable safe time margin)
- caching `FileIO` instances keyed by data token (via `RESTTokenFileIO`)
  and evicting them when tokens expire
- not mixing data tokens or auth state across different catalog instances or
  tables in the same process

### Engine or Embedding Application

Query engines (Flink, Spark, Hive, Trino, StarRocks, etc.) and applications
may expose only a subset of Paimon capabilities to users. They are responsible
for their own user-facing authorization boundaries unless Paimon explicitly
documents otherwise.

### Table Writer or Maintainer

This role may already have legitimate power to write or replace table
metadata, write or delete data files, manage snapshots, create or delete
branches and tags, and invoke destructive maintenance operations (compaction,
expiration, rollback). If a report only shows a new way to achieve the same
effect this role can already cause legitimately, it is usually not a security
issue in Paimon.

## Trust Boundaries

### Boundary 1: Operator-Trusted Configuration

The following are generally treated as trusted operator or deployment inputs:

- catalog properties (including `uri`, `warehouse`, `token.provider`)
- REST Catalog server endpoint configuration
- warehouse and storage roots
- authentication credentials
- Kerberos keytab paths and principal names
  (`security.kerberos.login.keytab`, `security.kerberos.login.principal`)
- metastore wiring (Hive Metastore URI, JDBC connection strings)
- custom HTTP headers (`header.*`)

If a report depends on the attacker controlling those values directly, it is
usually not a vulnerability in Paimon itself.

### Boundary 2: Catalog-Supplied Metadata

Paimon often accepts metadata locations, table properties, database
properties, schema definitions, and related control-plane information from a
catalog or metastore. By default, Paimon treats those sources as trusted.

This means a malicious catalog supplying incorrect or malicious metadata is
usually not a Paimon vulnerability by itself.

### Boundary 3: REST Catalog Server-Supplied Configuration and Delegated Storage Access

In REST deployments, Paimon accepts the following from the REST Catalog server:

- **Server configuration**: merged into client options via the `/v1/config`
  endpoint, including catalog prefix and additional headers
- **Data tokens**: time-limited storage credentials returned by the
  `/v1/{prefix}/databases/{database}/tables/{table}/token` endpoint, used by
  `RESTTokenFileIO` to access the underlying object store
- **Auth table query responses**: row-level filters and column masking rules
  returned by the `/v1/{prefix}/databases/{database}/tables/{table}/auth`
  endpoint

By default, these are treated as trusted control-plane inputs unless Paimon
explicitly documents a stronger guarantee.

This means a malicious REST Catalog server sending dangerous configuration or
overly broad data tokens is usually not a Paimon vulnerability by itself. It
also means many client-side token-selection bugs are often correctness or
specification issues rather than security boundary failures.

The major exception is **secret exposure**. If Paimon surfaces credentials or
secrets to a new audience that was not already trusted with them, that is
security-relevant. In particular:

- Data tokens for one table leaking to operations on a different table
- Auth state from one catalog instance leaking into another
- Credentials appearing in logs, error messages, or serialized state

### Boundary 4: Storage-Level Authorization

Object store permissions (e.g., OSS, S3, HDFS ACLs) are enforced by the
storage provider and the credentials the surrounding deployment chooses to
hand to Paimon. Paimon is not the root authority for bucket- or object-level
authorization.

Reports that depend primarily on over-broad IAM policies or permissive
storage ACLs are usually deployment-sensitive rather than product-security
issues in Paimon.

### Boundary 5: Engine-Level User Authorization

Paimon integrations may surface data and operations through a query engine or
application, but Paimon is not a complete user-authorization framework for
those systems.

Paimon does provide a mechanism for the REST Catalog server to supply
row-level filters and column masking rules via `authTableQuery`, but
enforcement of those rules is a shared responsibility between the engine
integration and the catalog server. Paimon relays the rules; the engine
must apply them.

## In-Scope Security Vulnerabilities

The following categories are generally security-relevant in Paimon when the
report is credible and reproducible.

### 1. Secret or Credential Disclosure to a New Audience

Examples include:

- catalog credentials exposed through a user-visible engine surface 
  (e.g., query results, EXPLAIN output, table properties)
- one catalog's credentials or auth state leaking into another catalog or
  session within the same process
- data tokens for table A being used for (or exposed to) table B
- credentials or tokens logged at INFO or lower levels without redaction
- credentials surviving in serialized `RESTTokenFileIO` or `RESTApi` state
  beyond their intended scope

### 2. Paimon-Owned Trust-Boundary Violations

Security issues exist when Paimon itself is expected to separate catalogs,
principals, or sessions and fails to do so.

Examples include:

- process-global auth provider or signer state crossing catalog instances
  (e.g., the `FILE_IO_CACHE` in `RESTTokenFileIO` returning a `FileIO`
  belonging to a different principal)
- a data token obtained for one table being reused for a different table's
  data access
- auth header state from one `RESTApi` instance leaking into another

### 3. Row-Level and Column-Level Access Control Bypass

If Paimon's client-side handling of `authTableQuery` responses (row filters
or column masking rules) allows a caller to bypass filters that the server
intended to enforce, that is security-relevant when the bypass occurs within
Paimon-owned code rather than in the engine integration.

## Usually Out of Scope or Non-Security by Default

These categories may still be real bugs worth fixing, but they are not usually
security vulnerabilities in Paimon itself.

### 1. Correctness Bugs

Examples:

- wrong byte offsets or stale decoded values in file formats
- incorrect merge-tree compaction producing wrong query results
- race conditions or logic bugs that do not create a new trust-boundary
  violation
- snapshot or schema version conflicts that produce incorrect metadata

### 2. Parser Hardening and Malformed-Input Robustness

Malformed-input crashes, raw runtime exceptions from invalid JSON or Avro
data, and memory amplification from oversized manifests or schemas are usually
treated as robustness or hardening work rather than security issues in Paimon
itself.

### 3. Malicious Catalog, Metastore, or External Service Scenarios

Reports that require a malicious catalog, metastore, REST Catalog server, or
other external service are usually outside Paimon's primary security boundary.

Examples:

- a REST Catalog server returning a data token with overly broad storage
  permissions
- a Hive Metastore returning a table location pointing to a sensitive path
- a REST Catalog server returning malicious row filters designed to extract
  data through side channels

### 4. Equivalent-Harm Reports

If the actor already has a legitimate capability that can cause the same harm,
the new path is usually not a security issue. This often applies to writers or
maintainers who already control metadata layout, file layout, or destructive
maintenance operations (snapshot expiration, orphan file cleanup, branch
deletion).

### 5. Denial of Service Through Normal Operations

Resource exhaustion caused by legitimate but expensive operations (e.g., large
compaction, scanning many partitions, listing all snapshots) is usually
treated as an operational concern rather than a security vulnerability.

## REST Catalog Specific Security Considerations

### Authentication

Paimon's REST Catalog client supports pluggable authentication through the
`AuthProvider` interface.

Authentication providers are created via the `AuthProviderFactory` SPI, loaded
using Java's `ServiceLoader` mechanism based on the `token.provider`
configuration. The authentication provider is process-level per catalog
instance and must not share mutable state across instances.

### Data Token Lifecycle

When `data-token.enabled` is `true`, `RESTTokenFileIO` manages delegated
storage credentials:

1. The client calls the table token endpoint to obtain a time-limited data
   token
2. The token is cached and used to construct a `FileIO` instance for storage
   access
3. Tokens are refreshed before expiration (1 hour safe time margin by default)
4. `FileIO` instances are cached in a process-global cache
   (`FILE_IO_CACHE`) keyed by `RESTToken`, with a maximum size of 1000
   entries and 10-hour expiry

Security-relevant invariants:

- Data tokens must be scoped to specific tables by the server
- The `FILE_IO_CACHE` keys on the full `RESTToken` (token content +
  expiration), so different tokens produce different `FileIO` instances
- Token refresh creates a new `RESTApi` instance from the catalog context if
  the original instance is unavailable (e.g., after deserialization)

### Kerberos

Paimon supports Kerberos authentication for Hadoop-based deployments through
`SecurityContext` and `SecurityConfiguration`. Keytab paths and principals
are treated as trusted operator configuration.

## Scanner Calibration Rules

A scanner targeting Paimon should treat a finding as higher-confidence only if
it plausibly shows one of the following:

- exposure of a secret or delegated credential to a new audience
- creation of a new unauthorized capability in a Paimon-owned component
- violation of a Paimon-owned trust boundary (e.g., cross-catalog credential
  leak, cross-table data token reuse)

A finding should be downgraded or rejected by default if it instead depends
primarily on:

- malformed-input robustness or denial-of-service behavior
- a malicious catalog, metastore, REST Catalog server, or external service
- a principal that already has equivalent power through legitimate write or
  maintenance capabilities
- operator misconfiguration (overly broad credentials, missing TLS, etc.)
