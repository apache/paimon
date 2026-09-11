---
title: "Release Overview"
sidebar_position: 1
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

# Release Overview

Paimon and PyPaimon share a release version and a community vote. This page
explains what is released and who does each part of the work. Use the linked
guides for commands and verification steps.

| Your task | Guide | Result |
| --- | --- | --- |
| Manage a release candidate (RC) | [Creating a release](./creating-a-release.md) | Signed source archives, a closed Java staging repository, and a vote thread |
| Review a candidate | [Verifying a release candidate](./verifying-a-release-candidate.md) | An independent vote with the checks and environment recorded |
| Publish an approved candidate | [Publishing a release](./publishing-a-release.md) | Public artifacts, documentation, release notes, and announcement |

## Release lifecycle

![Release lifecycle: prepare and tag a candidate, stage its artifacts, verify and vote, then publish the approved candidate. A failed candidate returns to preparation with a new RC number.](/img/project/release-lifecycle.svg)

[Open the lifecycle diagram at full size](/img/project/release-lifecycle.svg).

1. The Release Manager (RM) agrees the scope with the community and creates a
   signed RC tag.
2. GitHub Actions packages convenience artifacts. The RM signs and stages the
   Java artifacts and both source archives from the same tag.
3. Community members independently verify the candidate and vote. The RM
   records the result.
4. After approval, the RM publishes the approved artifacts and updates the
   documentation and website before announcing the release. A failed
   candidate needs a new RC number and a new vote.

The process follows the [ASF Release Policy](https://www.apache.org/legal/release-policy.html)
and [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html).

:::warning Preserve the approved candidate

The signed source archives are the Apache releases. Maven and PyPI packages
are convenience artifacts. Promote the source archives and closed Java
repository approved by the vote; do not rebuild or replace them. The final
PyPI package is built from the approved commit with the final version.

:::

## Release model

The combined Paimon and PyPaimon release uses one shared version number. The
Maven project version and `paimon-python/setup.py` version must be equal.

| Deliverable | Candidate | Published location |
| --- | --- | --- |
| Paimon source | `apache-paimon-PAIMON_VERSION-src.tgz`, `.asc`, `.sha512` | ASF distribution |
| Java convenience artifacts | Maven artifacts built in the JDK 8, 11, and 17 lanes | Apache Nexus staging, then Maven Central |
| PyPaimon source | `pypaimon-PAIMON_VERSION.tar.gz`, `.asc`, `.sha512` | ASF distribution |
| Python convenience package | `pypaimon==PAIMON_VERSIONrcRC_NUMBER` for an RC | TestPyPI, then `pypaimon==PAIMON_VERSION` on PyPI |

A combined release vote covers both signed source candidates. This guide does
not define an independent PyPaimon release. Before releasing PyPaimon
separately, the PMC must define a Python-only tag and workflow which do not
depend on the Maven version or Java jobs, and must provide a signed source
package which is independently sufficient to build and test the release.

### Java build matrix

The three Java lanes are different release targets, not interchangeable build
JDKs:

| JDK | Maven profiles and scope | Main artifacts |
| --- | --- | --- |
| 8 | `spark3,flink1` and the default reactor | Paimon core, Flink 1.x, Spark 3.x, Hive, filesystems, bundles, and other Java 8 artifacts |
| 11 | `flink2` plus `paimon-iceberg` | Flink 2.x, `paimon-flink2-common`, and Iceberg integration |
| 17 | `spark4` | Spark 4.x and its Scala 2.13 common artifacts |

Each lane must use the matching JDK. Building everything on JDK 17 with a lower
compiler target is not a substitute for running the JDK 8 and JDK 11 lanes.

## GitHub Actions release workflow

The release process uses the
[Release workflow](https://github.com/apache/paimon/actions/workflows/release.yml)
to package the JDK 8, JDK 11, and JDK 17 Java lanes and PyPaimon from every
signed RC tag. The Java lanes are merged into one unsigned Maven repository
image. The RM downloads that image, signs it, and stages it in Nexus. The RM
also creates and signs the two ASF source archives locally from the same tag.
The RM's GPG private key is never stored in GitHub Actions.

The workflow has the following contract:

| Job | Required behavior |
| --- | --- |
| Validation | Require an RC tag named `release-PAIMON_VERSION-rcN` or a final tag named `release-PAIMON_VERSION`, where `PAIMON_VERSION` exactly equals the root Maven `project.version` |
| Java 8 | Use Temurin 8 to deploy the default reactor with Spark 3 and Flink 1 into a local Maven repository image |
| Java 11 | Use Temurin 11 to deploy Flink 2 and Iceberg into a local Maven repository image |
| Java 17 | Use Temurin 17 to deploy Spark 4 into a local Maven repository image |
| Java repository | Require every deploy-enabled effective-POM project and its POM, main JAR, and source JAR; retain Javadoc JARs where Maven produces them; merge all three lanes; reject conflicting coordinates; then upload the complete unsigned Maven repository image, checksums, manifests, and logs |
| Python package | Build and validate the PyPaimon source distribution and universal wheel, then upload them as workflow artifacts |
| Python publish | Publish an RC to TestPyPI or a final tag to PyPI after Python packaging passes, without waiting for Java packaging |

Before packaging, every Java lane runs Maven Enforcer's
`requireReleaseVersion` and `requireReleaseDeps` rules over its complete reactor
scope. The latter includes transitive dependencies. Any remaining
`-SNAPSHOT` project, parent, direct dependency, or transitive dependency is a
release blocker.

The Java jobs run independently of the common validation and Python jobs. They
use `-Dgpg.skip=true`, deploy only to runner-local file repositories, and never
receive Nexus credentials or a GPG private key. The combined repository image
contains POMs, main artifacts, source JARs, Javadoc JARs produced by Maven, and
Maven-generated checksums. Scala-only and wrapper modules may not produce a
Javadoc JAR. The image is the input to the RM's local signing and Nexus
staging steps, not itself an ASF release. The Python RC job uses the
`TEST_PYPI_API_TOKEN` repository Actions secret to publish
`PAIMON_VERSIONrcRC_NUMBER` to TestPyPI. The final job uses the
`PYPI_API_TOKEN` repository Actions secret to publish to PyPI. The release
workflow passes only these two secrets to the reusable publishing workflow.

## Artifact flow

![A signed RC tag feeds three artifact paths: locally signed sources staged in ASF dist dev, a Java repository signed locally and closed in Nexus, and Python packages published to TestPyPI. After approval, sources and Java artifacts are promoted, while the final signed tag builds the final PyPI version.](/img/project/release-artifacts.svg)

[Open the artifact diagram at full size](/img/project/release-artifacts.svg).

Source and Java publication preserve the approved bytes. Python publication
builds the final PyPI version from the final tag at the approved RC commit.
All three paths must be checked before the release announcement.

## Responsibilities and handoffs

| Owner | Responsibility | Evidence to hand off |
| --- | --- | --- |
| RM | Review relevant CI results and prepare an immutable candidate | Signed tag, commit SHA, and release versions |
| GitHub Actions | Package Java lanes and Python artifacts | Run URL, manifests, checksums, logs, and TestPyPI version |
| RM | Sign locally and stage the candidate | Both source URLs, signing-key fingerprint, and closed Nexus repository URL |
| Voters | Independently inspect, build, and test the source candidate | Vote with actual checks, platforms, tool versions, and any failures |
| RM | Tally the vote and publish after approval | Vote result, final tag, public downloads, and updated documentation |

The Java packaging lanes do not run the full test matrix. The RM reviews
relevant CI results before calling the vote, and voters report their own
source build and test scope. Packaging success alone is not release approval.
