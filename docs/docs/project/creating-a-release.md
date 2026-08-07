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

## One-time RM setup

Before managing the first release:

1. Create an RSA GPG key of at least 2048 bits associated with your
   `@apache.org` identity and publish it to a public key server. The
   [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html)
   requires RSA keys for new artifacts; use RSA 4096 for a newly generated
   release key. Do not use DSA, ECDSA, EdDSA, or Ed25519 to sign release
   artifacts.
2. Append the public key to the Paimon
   [KEYS](https://downloads.apache.org/paimon/KEYS) file. Never remove keys
   required to verify an older release.
3. Configure Git and local GPG to sign tags and Maven artifacts with the same
   key.
4. Configure the local Maven server ID `apache.releases.https` with the RM's
   Apache Nexus credentials. Do not copy the GPG key or Nexus credentials into
   GitHub Actions.
5. Record Paimon's Apache Nexus staging profile ID. The local repository upload
   script requires it as `STAGING_PROFILE_ID`.
6. Confirm access to the ASF distribution SVN repository, Apache Nexus,
   TestPyPI, and PyPI.
7. Verify that the repository Actions secrets `TEST_PYPI_API_TOKEN` and
   `PYPI_API_TOKEN` are configured without printing their values in an Actions
   log.

```shell
gpg --with-subkey-fingerprint --list-secret-keys --keyid-format LONG
git config user.signingkey
svn --version
```

Confirm that the exact key or signing subkey configured for the release is
reported as `rsa2048` or larger. Do not infer compliance from another key in
the same keyring.

## Prepare the release

### Agree on the release

Discuss the release on `dev@paimon.apache.org`, select an RM, resolve release
blockers, review incompatible changes and upgrade notes, and prepare release
notes. Review the CI status of the commit from which the candidate will be cut;
the RM decides whether any unresolved failures block the release.

### Set the release variables

For the 2.0.0 release, use matching Java and Python versions:

```shell
PAIMON_VERSION="2.0.0"
RC_NUMBER="1"
RELEASE_BRANCH="release-2.0"

RC_REF="release-${PAIMON_VERSION}-rc${RC_NUMBER}"
RELEASE_TAG="release-${PAIMON_VERSION}"
```

Use these exact values in the local working branch, tag, workflow inputs, SVN
directories, vote email, and Java package manifests.

### Work from a clean clone

```shell
git clone https://github.com/apache/paimon.git paimon-release
cd paimon-release
git checkout "${RELEASE_BRANCH}"
git pull --ff-only origin "${RELEASE_BRANCH}"
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

The remote release branch is not frozen by an RC. It may continue receiving
fixes for a later RC or maintenance release, and its head does not need to stay
equal to the published RC tag. If a later branch change must be included in the
release currently under vote, create a new RC from that updated branch state;
never move or replace the existing RC tag.

Change all Maven modules from `PAIMON_VERSION-SNAPSHOT` to
`PAIMON_VERSION`. The helper commits the Maven version change:

```shell
NEW_VERSION="${PAIMON_VERSION}" \
  ./tools/releasing/update_branch_version.sh
```

Set `VERSION` in `paimon-python/setup.py` to the final
`PAIMON_VERSION`, without `.dev` or an RC suffix. The release workflow
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
  -m "Apache Paimon ${PAIMON_VERSION} and PyPaimon ${PAIMON_VERSION} RC${RC_NUMBER}"
git tag -v "${RC_REF}"

git push origin \
  "refs/tags/${RC_REF}:refs/tags/${RC_REF}"
```

Pushing the signed tag starts all three Java packaging lanes from the exact same
commit. Python publishing starts as soon as common validation and Python
packaging succeed; it does not wait for Java packaging. Record:

- the workflow run URL and `head_sha`;
- the `java-release-repository` artifact name, manifest, and SHA-512 checksum;
- the TestPyPI project/version URL.

Unrelated CI status is not an automatic release gate; the RM decides whether a
failure blocks the candidate. The three Java packaging lanes and repository
merge must succeed before their repository image can be signed and staged.

## Sign and stage the Java convenience artifacts locally

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

Download the combined repository image from the recorded workflow run. With
GitHub CLI, set `RUN_ID` to the numeric run ID from the workflow URL:

```shell
RUN_ID="<GITHUB_WORKFLOW_RUN_ID>"
JAVA_REPOSITORY_ARCHIVE="paimon-${PAIMON_VERSION}-maven-repository.tar.gz"

gh run download "${RUN_ID}" \
  --name java-release-repository \
  --dir java-release-repository
```

Verify the archive and every file in the repository image before signing it:

```shell
(
  cd java-release-repository
  sha512sum -c "${JAVA_REPOSITORY_ARCHIVE}.sha512"
)

mkdir paimon-maven-repository
tar -xzf "java-release-repository/${JAVA_REPOSITORY_ARCHIVE}" \
  -C paimon-maven-repository
(
  cd paimon-maven-repository
  sha512sum -c \
    ../java-release-repository/paimon-maven-repository-sha512.txt
)

grep -Fx "version=${PAIMON_VERSION}" \
  java-release-repository/paimon-maven-repository-manifest.txt
grep -Fx "tag=${RC_REF}" \
  java-release-repository/paimon-maven-repository-manifest.txt
grep -Fx "commit=$(git rev-parse HEAD)" \
  java-release-repository/paimon-maven-repository-manifest.txt
```

On macOS, replace `sha512sum` with `shasum -a 512`. Sign every JAR and POM in
the extracted repository with the RM's local key. `GPG_KEY_ID` is optional when
the default GPG key is the release key:

```shell
REPOSITORY_DIRECTORY="${PWD}/paimon-maven-repository" \
GPG_KEY_ID="<RELEASE_GPG_KEY_ID>" \
  ./tools/releasing/sign_maven_repository.sh
```

Upload the complete signed image with the Nexus Staging Maven Plugin:

```shell
REPOSITORY_DIRECTORY="${PWD}/paimon-maven-repository" \
STAGING_PROFILE_ID="<PAIMON_STAGING_PROFILE_ID>" \
  ./tools/releasing/deploy_maven_repository.sh
```

The upload plugin creates one staging repository and closes it after a complete
upload. The script explicitly disables automatic release. On a transport
failure it drops the partial staging repository, so a retry starts clean instead
of splitting coordinates across repositories. On a close-rule failure the
script keeps the repository for inspection. Record the single
`orgapachepaimon-XXXX` repository ID. If closing fails, inspect and drop that
repository, correct the cause, and upload the clean signed image again. Start
the vote only after one staging repository is closed successfully. Do not
release it before the vote passes.

If signing is interrupted, discard the extracted repository and extract the
verified archive again before retrying. If uploading is interrupted, first
confirm in Nexus that the plugin dropped the partial staging repository; drop
it manually if necessary, then rerun the upload with the unchanged signed
repository image.

## Stage the source candidates

Create both source candidates locally from the exact signed tag in a fresh
clone. The Paimon helper creates, signs, and checksums the main source archive.
It also rejects unsafe archive paths and hidden macOS AppleDouble metadata.
Build the PyPaimon source distribution separately and sign it with the same RM
key:

```shell
git checkout --detach "refs/tags/${RC_REF}"

RELEASE_VERSION="${PAIMON_VERSION}" \
  ./tools/releasing/create_source_release.sh

cd paimon-python
python3 setup.py sdist
gpg --armor --detach-sig \
  "dist/pypaimon-${PAIMON_VERSION}.tar.gz"

(
  cd dist
  if command -v sha512sum >/dev/null 2>&1; then
    sha512sum "pypaimon-${PAIMON_VERSION}.tar.gz" \
      > "pypaimon-${PAIMON_VERSION}.tar.gz.sha512"
  else
    shasum -a 512 "pypaimon-${PAIMON_VERSION}.tar.gz" \
      > "pypaimon-${PAIMON_VERSION}.tar.gz.sha512"
  fi
)

cp "dist/pypaimon-${PAIMON_VERSION}.tar.gz"* ../release/
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

mkdir "paimon-dist-dev/pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}"
cp "release/pypaimon-${PAIMON_VERSION}.tar.gz"* \
  "paimon-dist-dev/pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}/"

svn add \
  "paimon-dist-dev/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "paimon-dist-dev/pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}"
svn commit -m \
  "Stage Paimon ${PAIMON_VERSION} and PyPaimon ${PAIMON_VERSION} RC${RC_NUMBER}" \
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
Subject: [VOTE] Release Apache Paimon ${PAIMON_VERSION} and PyPaimon ${PAIMON_VERSION} (RC${RC_NUMBER})

Hi everyone,

Please review and vote on Apache Paimon ${PAIMON_VERSION} and
PyPaimon ${PAIMON_VERSION}, release candidate ${RC_NUMBER}.

[ ] +1 Approve
[ ]  0 No opinion
[ ] -1 Do not approve (please explain)

Paimon source candidate:
https://dist.apache.org/repos/dist/dev/paimon/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}/

PyPaimon source candidate:
https://dist.apache.org/repos/dist/dev/paimon/pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}/

Signed Git tag:
release-${PAIMON_VERSION}-rc${RC_NUMBER}

Commit:
https://github.com/apache/paimon/commit/<RC_COMMIT_SHA>

GitHub Actions release run:
<WORKFLOW_RUN_URL>

KEYS:
https://downloads.apache.org/paimon/KEYS

Closed Java staging repository:
<JAVA_NEXUS_URL>

PyPaimon RC:
https://test.pypi.org/project/pypaimon/${PAIMON_VERSION}rc${RC_NUMBER}/

Verification guide:
https://github.com/apache/paimon/blob/release-${PAIMON_VERSION}-rc${RC_NUMBER}/docs/docs/project/verifying-a-release-candidate.md

The vote will remain open for at least 72 hours.
```

After the deadline, tally binding and non-binding votes separately and send
`[RESULT][VOTE]` to the same thread.

## Replace a failed candidate

If the vote finds a problem:

1. Fix it through the normal review process.
2. Drop the Nexus staging repository belonging to the failed RC.
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

svn mv -m "Release PyPaimon ${PAIMON_VERSION}" \
  "https://dist.apache.org/repos/dist/dev/paimon/pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "https://dist.apache.org/repos/dist/release/paimon/pypaimon-${PAIMON_VERSION}"
```

### Promote convenience artifacts

1. In Nexus, confirm that the recorded Java staging repository is still closed
   and has the exact artifact tree approved by the vote.
2. Release that exact closed repository to Maven Central. Do not upload or
   rebuild the Java artifacts again.
3. Confirm that the final tag's PyPI publish job builds
   `pypaimon==PAIMON_VERSION` from the approved tag commit and does not change
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
