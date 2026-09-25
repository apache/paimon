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

package org.apache.paimon.testutils.junit;

/**
 * Utility class for defining the image names and versions of Docker containers used during the Java
 * tests. The names/versions are centralised here in order to make testing version updates easier.
 */
public class DockerImageVersions {

    // MinIO archived its community edition and removed the images from Docker Hub and quay.io, so
    // quay.io/minio/minio is no longer anonymously pullable. pgsty/silo is a drop-in MinIO fork
    // that keeps the S3 API, the MINIO_* env vars, and the /minio/* routes, and stays published.
    public static final String MINIO = "docker.io/pgsty/silo:RELEASE.2026-09-03T13-18-01Z";
}
