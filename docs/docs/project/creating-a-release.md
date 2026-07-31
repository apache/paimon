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
to package Java and PyPaimon from every signed RC tag. The RM signs and stages
Java locally, and creates and signs the two ASF source archives locally from
the same tag. The RM's GPG private key is never stored in GitHub Actions.

The workflow has the following contract:

| Job | Required behavior |
| --- | --- |
| Validation | Require an RC tag named `release-PAIMON_VERSION-rcN` or a final tag named `release-PAIMON_VERSION`, where `PAIMON_VERSION` exactly equals the root Maven `project.version` |
| Java 8 | Use Temurin 8 to package the default reactor with Spark 3 and Flink 1, then upload the package, checksums, manifest, and log |
| Java 11 | Use Temurin 11 to package Flink 2 and Iceberg, then upload the package, checksums, manifest, and log |
| Java 17 | Use Temurin 17 to package Spark 4, then upload the package, checksums, manifest, and log |
| Python package | Build and validate the PyPaimon source distribution and universal wheel, then upload them as workflow artifacts |
| Python publish | Publish an RC to TestPyPI only after all Java and Python packaging jobs pass; publish a final tag to PyPI |

Before packaging, every Java lane runs Maven Enforcer's
`requireReleaseVersion` and `requireReleaseDeps` rules over its complete reactor
scope. The latter includes transitive dependencies. Any remaining
`-SNAPSHOT` project, parent, direct dependency, or transitive dependency is a
release blocker.

The Java jobs use `-Dgpg.skip=true` and never receive Nexus credentials or a
GPG private key. Their artifacts are build evidence, not the Maven staging
repositories used for the vote. The Python RC job uses the
`TEST_PYPI_API_TOKEN` repository Actions secret to publish
`PYPAIMON_VERSIONrcRC_NUMBER` to TestPyPI. The final job uses the
`PYPI_API_TOKEN` repository Actions secret to publish to PyPI. The release
workflow passes only these two secrets to the reusable publishing workflow.

## One-time RM setup

Before managing the first release:

1. Create a GPG key associated with your `@apache.org` identity and publish it
   to a public key server.
2. Append the public key to the Paimon
   [KEYS](https://downloads.apache.org/paimon/KEYS) file. Never remove keys
   required to verify an older release.
3. Configure Git and local GPG to sign tags and Maven artifacts with the same
   key.
4. Configure the local Maven server ID `apache.releases.https` with the RM's
   Apache Nexus credentials. Do not copy the GPG key or Nexus credentials into
   GitHub Actions.
5. Confirm access to the ASF distribution SVN repository, Apache Nexus,
   TestPyPI, and PyPI.
6. Verify that the repository Actions secrets `TEST_PYPI_API_TOKEN` and
   `PYPI_API_TOKEN` are configured without printing their values in an Actions
   log.

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

For the 2.0.0 release, use matching Java and Python versions:

```shell
PAIMON_VERSION="2.0.0"
PYPAIMON_VERSION="2.0.0"
RC_NUMBER="1"

RC_REF="release-${PAIMON_VERSION}-rc${RC_NUMBER}"
RELEASE_TAG="release-${PAIMON_VERSION}"
```

Use these exact values in the local working branch, tag, workflow inputs, SVN
directories, vote email, and Java package manifests.

### Work from a clean clone

```shell
git clone https://github.com/apache/paimon.git paimon-release
cd paimon-release
git checkout master
git pull --ff-only origin master
git status --short
```

The last command must produce no output.

### Create a local RC branch and set versions

Create a local RC working branch with the existing helper:

```shell
RELEASE_VERSION="${PAIMON_VERSION}" \
RELEASE_CANDIDATE="${RC_NUMBER}" \
  ./tools/releasing/create_release_branch.sh
```

This creates the local branch `release-PAIMON_VERSION-rcRC_NUMBER`. Use it only
to prepare the candidate commit; do not push this branch to the remote.

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

The signed tag is the only RC ref published to the remote. Do not push the
same-named local RC branch. Use an explicit tag ref when pushing:

```shell
git tag -s "${RC_REF}" \
  -m "Apache Paimon ${PAIMON_VERSION} and PyPaimon ${PYPAIMON_VERSION} RC${RC_NUMBER}"
git tag -v "${RC_REF}"

git push origin \
  "refs/tags/${RC_REF}:refs/tags/${RC_REF}"
```

Pushing the signed tag starts the Release workflow. The common validation job
must succeed before any Java or Python packaging or publishing job can run.
Wait for every required job to succeed. Record:

- the workflow run URL and `head_sha`;
- the JDK 8, 11, and 17 package artifact names, manifests, and SHA-512
  checksums;
- the TestPyPI project/version URL.

Do not start the vote when a required lane is missing or has been rerun from a
different commit.

## Stage the Java convenience artifacts locally

Check out the exact signed RC tag in a clean clone. Use the RM machine's local
GPG key and Maven credentials; do not download a private key into a CI runner.
Use Maven 3.8.8, matching the Release workflow. Export `GPG_TTY` when the local
GPG agent needs terminal access:

```shell
git checkout --detach "refs/tags/${RC_REF}"
export GPG_TTY="$(tty)"

gpg --list-secret-keys --keyid-format LONG
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
```

Download the three Java package artifacts from the recorded workflow run and
verify each `*-packages.tar.gz.sha512` file. Inspect each manifest and artifact
inventory to confirm that the lane, version, commit, JDK, and module scope match
the signed RC tag. The workflow packages are CI build evidence; do not compare
their individual JAR checksums with the locally built and signed JARs. Archive
entry order and other build metadata can make independently built JARs differ
at the byte level.

Run each existing staging script under its required JDK. Confirm the output of
`java -version` and `mvn -version` before every command:

```shell
# JDK 8: default reactor, Flink 1.x, and Spark 3.x
./tools/releasing/deploy_staging_jars.sh

# JDK 11: Flink 2.x and Iceberg
./tools/releasing/deploy_staging_jars_for_jdk11.sh

# JDK 17: Spark 4.x
./tools/releasing/deploy_staging_jars_for_jdk17.sh
```

Maven signs the release artifacts with the RM's local GPG key and deploys them
using the local `apache.releases.https` server credentials. After each command,
record the `orgapachepaimon-XXXX` repository ID and confirm that it contains
only the intended lane. Close each repository and resolve every close-time rule
failure before starting the vote. Closing freezes the candidate that voters
inspect. Do not release a repository before the vote passes.

## Stage the source candidates

Create both source candidates locally from the exact signed tag in a fresh
clone. The Paimon helper creates, signs, and checksums the main source archive.
Build the PyPaimon source distribution separately and sign it with the same RM
key:

```shell
git checkout --detach "refs/tags/${RC_REF}"

RELEASE_VERSION="${PAIMON_VERSION}" \
  ./tools/releasing/create_source_release.sh

cd paimon-python
python3 setup.py sdist
gpg --armor --detach-sig \
  "dist/pypaimon-${PYPAIMON_VERSION}.tar.gz"

if command -v sha512sum >/dev/null 2>&1; then
  sha512sum "dist/pypaimon-${PYPAIMON_VERSION}.tar.gz" \
    > "dist/pypaimon-${PYPAIMON_VERSION}.tar.gz.sha512"
else
  shasum -a 512 "dist/pypaimon-${PYPAIMON_VERSION}.tar.gz" \
    > "dist/pypaimon-${PYPAIMON_VERSION}.tar.gz.sha512"
fi

cp "dist/pypaimon-${PYPAIMON_VERSION}.tar.gz"* ../release/
cd ..
```

Verify both signatures and checksum files locally before uploading them to ASF
dist dev.

```shell
svn checkout --depth=immediates \
  https://dist.apache.org/repos/dist/dev/paimon/ paimon-dist-dev

mkdir "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}"
cp "release/apache-paimon-${PAIMON_VERSION}-src.tgz"* \
  "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}/"

mkdir "paimon-dist-dev/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}"
cp "release/pypaimon-${PYPAIMON_VERSION}.tar.gz"* \
  "paimon-dist-dev/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}/"

svn add \
  "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "paimon-dist-dev/pypaimon-${PYPAIMON_VERSION}-rc${RC_NUMBER}"
svn commit -m \
  "Stage Paimon ${PAIMON_VERSION} and PyPaimon ${PYPAIMON_VERSION} RC${RC_NUMBER}" \
  paimon-dist-dev
```

Never overwrite an existing RC directory. TestPyPI versions are immutable for
this release process as well: the workflow checks for an existing version before
upload and fails instead of skipping files. The uploader also rejects duplicate
files to cover races after the check. If any byte changes, create a new RC with
a new RC number.

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

Signed Git tag:
release-${PAIMON_VERSION}-rc${RC_NUMBER}

Commit:
https://github.com/apache/paimon/commit/<RC_COMMIT_SHA>

GitHub Actions release run:
<WORKFLOW_RUN_URL>

KEYS:
https://downloads.apache.org/paimon/KEYS

Closed Java staging repositories:
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
4. Increment `RC_NUMBER`; never reuse the failed candidate's TestPyPI version.
5. Create a new local RC working branch and signed tag, push only the tag, run
   every workflow lane again, stage new source candidates, and start a new
   72-hour vote.

## Finalize an approved release

### Create the final signed tag

The final tag must point to exactly the approved RC commit:

```shell
git tag -s "${RELEASE_TAG}" "refs/tags/${RC_REF}^{commit}" \
  -m "Release Apache Paimon ${PAIMON_VERSION}"

test "$(git rev-parse "refs/tags/${RC_REF}^{commit}")" = \
     "$(git rev-parse "refs/tags/${RELEASE_TAG}^{commit}")"
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

1. In Nexus, confirm that every recorded JDK 8, 11, and 17 staging repository
   is still closed and has the exact artifact tree approved by the vote.
2. Release those exact closed repositories to Maven Central. Do not run Maven
   deploy again.
3. Confirm that the final tag's PyPI publish job builds
   `pypaimon==PYPAIMON_VERSION` from the approved tag commit and does not change
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
