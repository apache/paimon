---
title: "Creating a Release"
sidebar_position: 2
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

# Creating a Release

This guide is for the Release Manager (RM) of Apache Paimon and PyPaimon. It
follows the [ASF Release Policy](https://www.apache.org/legal/release-policy.html)
and the [ASF Release Distribution Policy](https://infra.apache.org/release-distribution).

:::warning

The signed source archives are the Apache releases. Maven and PyPI packages are
convenience artifacts. The final release must promote the source files and Java
staging repositories approved by the community; do not rebuild or replace them
after the vote.

:::

## Release model

Paimon contains two independently versioned deliverables. Do not assume that
their version numbers are equal.

| Deliverable | Candidate | Published location |
| --- | --- | --- |
| Paimon source | `apache-paimon-PAIMON_VERSION-src.tgz`, `.asc`, `.sha512` | ASF distribution |
| Java convenience artifacts | Maven artifacts built in the JDK 8, 11, and 17 lanes | Apache Nexus staging, then Maven Central |
| PyPaimon source | `pypaimon-PYPAIMON_VERSION.tar.gz`, `.asc`, `.sha512` | ASF distribution |
| Python convenience package | `pypaimon==PYPAIMON_VERSIONrcRC_NUMBER` for an RC | TestPyPI, then `pypaimon==PYPAIMON_VERSION` on PyPI |

A combined release vote covers both signed source candidates. If PyPaimon is
released separately, use the same Python steps and hold a separate vote.

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

The release process uses a **Release** workflow in
[GitHub Actions](https://github.com/apache/paimon/actions) to package every
candidate from its signed RC tag. Before cutting an RC, confirm that this
workflow is present and enabled on the release branch. It must record the tag,
commit SHA, inputs, tool versions, artifact checksums, and Nexus staging
repository IDs.

The workflow has the following contract:

| Job | Required behavior |
| --- | --- |
| Source | Run `tools/releasing/create_source_release.sh`, and upload the Paimon archive, signature, and checksum without changing them |
| Java 8 | Use Temurin 8 and `tools/releasing/deploy_staging_jars.sh` |
| Java 11 | Use Temurin 11 and `tools/releasing/deploy_staging_jars_for_jdk11.sh` |
| Java 17 | Use Temurin 17 and `tools/releasing/deploy_staging_jars_for_jdk17.sh` |
| Python | Build the PyPaimon source distribution, sign and checksum it, run the supported Python test matrix, and publish the RC package to TestPyPI |
| Manifest | Collect the SHA-512 digest of every workflow artifact and all Nexus staging repository IDs in one release manifest |

The Java jobs deploy signed artifacts to Nexus staging but must not close or
release those repositories. The Python RC job publishes
`PYPAIMON_VERSIONrcRC_NUMBER` to TestPyPI. Stable PyPI publication and all
promotion steps remain disabled until the vote passes.

Configure the protected release environment with the Nexus credentials already
used by the snapshot workflows (`NEXUS_USER` and `NEXUS_PW`), the release GPG
private key and passphrase, and TestPyPI/PyPI credentials. Require an RM approval
for jobs that use publishing credentials.

## One-time RM setup

Before managing the first release:

1. Create a GPG key associated with your `@apache.org` identity and publish it
   to a public key server.
2. Append the public key to the Paimon
   [KEYS](https://downloads.apache.org/paimon/KEYS) file. Never remove keys
   required to verify an older release.
3. Configure Git to sign tags with the same key.
4. Confirm access to the ASF distribution SVN repository, Apache Nexus, GitHub
   release environments, TestPyPI, and PyPI.
5. Verify that all required release secrets are configured without printing
   their values in an Actions log.

```shell
gpg --list-secret-keys --keyid-format LONG
git config user.signingkey
svn --version
```

## Prepare the release

### Agree on the release

Discuss the release on `dev@paimon.apache.org`, select an RM, resolve release
blockers, review incompatible changes and upgrade notes, and prepare release
notes. Ensure CI is green on the commit from which the candidate will be cut.

### Set the release variables

The following example deliberately uses different Java and Python versions:

```shell
PAIMON_VERSION="2.0.0"
PYPAIMON_VERSION="1.5.0"
RC_NUMBER="1"

RC_REF="release-${PAIMON_VERSION}-rc${RC_NUMBER}"
RELEASE_TAG="release-${PAIMON_VERSION}"
```

Use these exact values in the branch, tag, workflow inputs, SVN directories,
vote email, and release manifest.

### Work from a clean clone

```shell
git clone https://github.com/apache/paimon.git paimon-release
cd paimon-release
git checkout master
git pull --ff-only origin master
git status --short
```

The last command must produce no output.

### Create the RC branch and set versions

Create the RC branch with the existing helper:

```shell
RELEASE_VERSION="${PAIMON_VERSION}" \
RELEASE_CANDIDATE="${RC_NUMBER}" \
  ./tools/releasing/create_release_branch.sh
```

This creates `release-PAIMON_VERSION-rcRC_NUMBER`.

Change all Maven modules from `PAIMON_VERSION-SNAPSHOT` to
`PAIMON_VERSION`. The helper commits the Maven version change:

```shell
NEW_VERSION="${PAIMON_VERSION}" \
  ./tools/releasing/update_branch_version.sh
```

Set `VERSION` in `paimon-python/setup.py` to the final
`PYPAIMON_VERSION`, without `.dev` or an RC suffix. The release workflow
derives the TestPyPI version by appending `rcRC_NUMBER`; the source candidate
keeps the final version.

Review and commit the Python version, dependency declarations, release notes,
`LICENSE`, `NOTICE`, and any generated legal files. Then confirm that the
candidate contains no snapshot or development version:

```shell
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
python3 paimon-python/setup.py --version
rg --glob 'pom.xml' "<version>${PAIMON_VERSION}-SNAPSHOT</version>"
rg '^VERSION = ".*\.dev' paimon-python/setup.py
git status --short
```

The first two commands must print the requested release versions, the `rg`
commands must find no release-version marker, and the worktree must be clean.

### Sign and push the RC tag

The historical Paimon convention uses the same name for the RC branch and tag.
Use explicit refs when pushing so Git cannot select the wrong one:

```shell
git tag -s "${RC_REF}" \
  -m "Apache Paimon ${PAIMON_VERSION} and PyPaimon ${PYPAIMON_VERSION} RC${RC_NUMBER}"
git tag -v "${RC_REF}"

git push origin \
  "refs/heads/${RC_REF}:refs/heads/${RC_REF}"
git push origin \
  "refs/tags/${RC_REF}:refs/tags/${RC_REF}"
```

Pushing the signed tag starts the Release workflow. Wait for every source,
Java, Python, and manifest job to succeed. Record:

- the workflow run URL and `head_sha`;
- the Paimon and PyPaimon source artifact SHA-512 digests;
- all JDK 8, 11, and 17 Nexus staging repository IDs;
- the TestPyPI project/version URL.

Do not start the vote when a required lane is missing or has been rerun from a
different commit.

## Stage the source candidates

Download the source artifacts from the successful workflow run and verify their
manifest before uploading them to ASF dist dev.

```shell
svn checkout --depth=immediates \
  https://dist.apache.org/repos/dist/dev/paimon/ paimon-dist-dev

mkdir "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}"
cp "apache-paimon-${PAIMON_VERSION}-src.tgz"* \
  "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}/"

mkdir "paimon-dist-dev/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}"
cp "pypaimon-${PYPAIMON_VERSION}.tar.gz"* \
  "paimon-dist-dev/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}/"

svn add \
  "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "paimon-dist-dev/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}"
svn commit -m \
  "Stage Paimon ${PAIMON_VERSION} and PyPaimon ${PYPAIMON_VERSION} RC${RC_NUMBER}" \
  paimon-dist-dev
```

Never overwrite an existing RC directory. If any byte changes, create a new RC.

## Call the vote

Send a plain-text vote to `dev@paimon.apache.org`. Keep it open for at least
72 hours. The vote must receive at least three binding `+1` votes and more
binding `+1` than binding `-1` votes.

```text
Subject: [VOTE] Release Apache Paimon ${PAIMON_VERSION} and PyPaimon ${PYPAIMON_VERSION} (RC${RC_NUMBER})

Hi everyone,

Please review and vote on Apache Paimon ${PAIMON_VERSION} and
PyPaimon ${PYPAIMON_VERSION}, release candidate ${RC_NUMBER}.

[ ] +1 Approve
[ ]  0 No opinion
[ ] -1 Do not approve (please explain)

Paimon source candidate:
https://dist.apache.org/repos/dist/dev/paimon/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}/

PyPaimon source candidate:
https://dist.apache.org/repos/dist/dev/paimon/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}/

Signed Git tag and commit:
https://github.com/apache/paimon/releases/tag/release-${PAIMON_VERSION}-rc${RC_NUMBER}
<RC_COMMIT_SHA>

GitHub Actions release run:
<WORKFLOW_RUN_URL>

KEYS:
https://downloads.apache.org/paimon/KEYS

Java staging repositories:
<JDK_8_NEXUS_URL>
<JDK_11_NEXUS_URL>
<JDK_17_NEXUS_URL>

PyPaimon RC:
https://test.pypi.org/project/pypaimon/${PYPAIMON_VERSION}rc${RC_NUMBER}/

Verification guide:
https://github.com/apache/paimon/blob/release-${PAIMON_VERSION}-rc${RC_NUMBER}/docs/docs/project/verifying-a-release-candidate.md

The vote will remain open for at least 72 hours.
```

After the deadline, tally binding and non-binding votes separately and send
`[RESULT][VOTE]` to the same thread.

## Replace a failed candidate

If the vote finds a problem:

1. Fix it through the normal review process.
2. Drop every Nexus staging repository belonging to the failed RC.
3. Remove the superseded dist-dev directories, or retain them temporarily when
   useful to the vote discussion. Never replace their contents.
4. Increment `RC_NUMBER`.
5. Create a new RC branch and signed tag, run every workflow lane again, stage
   new source candidates, and start a new 72-hour vote.

## Finalize an approved release

### Create the final signed tag

The final tag must point to exactly the approved RC commit:

```shell
git tag -s "${RELEASE_TAG}" "${RC_REF}^{commit}" \
  -m "Release Apache Paimon ${PAIMON_VERSION}"

test "$(git rev-parse "${RC_REF}^{commit}")" = \
     "$(git rev-parse "${RELEASE_TAG}^{commit}")"
git tag -v "${RELEASE_TAG}"
git push origin "refs/tags/${RELEASE_TAG}:refs/tags/${RELEASE_TAG}"
```

### Promote the source releases

Move, rather than copy or rebuild, both approved candidate directories:

```shell
svn mv -m "Release Apache Paimon ${PAIMON_VERSION}" \
  "https://dist.apache.org/repos/dist/dev/paimon/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "https://dist.apache.org/repos/dist/release/paimon/paimon-${PAIMON_VERSION}"

svn mv -m "Release PyPaimon ${PYPAIMON_VERSION}" \
  "https://dist.apache.org/repos/dist/dev/paimon/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}" \
  "https://dist.apache.org/repos/dist/release/paimon/pypaimon-${PYPAIMON_VERSION}"
```

### Promote convenience artifacts

1. In Nexus, close every recorded JDK 8, 11, and 17 staging repository. Inspect
   all rule failures and artifact trees before continuing.
2. Release those exact repositories to Maven Central. Do not run Maven deploy
   again.
3. Approve the protected PyPI promotion job for the final tag. It must build
   `pypaimon==PYPAIMON_VERSION` from the approved tag commit and must not change
   project source.
4. Verify Maven Central and PyPI before announcing the release.

### Publish and announce

Create a GitHub release from `release-PAIMON_VERSION`, review the generated
notes, and link both source releases. Update the Paimon download page and
versioned documentation. After ASF mirrors, Maven Central, and PyPI are all
available, announce the release to `dev@paimon.apache.org` and
`announce@apache.org` from an `@apache.org` address.

Remove superseded releases from the live ASF distribution area when required;
they remain available from the
[Apache archive](https://archive.apache.org/dist/paimon/).

See [Verifying a Release Candidate](./verifying-a-release-candidate.md) for the
voter checklist.
