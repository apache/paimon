---
title: "Verifying a Release Candidate"
sidebar_position: 3
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

# Verifying a Release Candidate

Verify a release candidate independently before voting. The signed Paimon and
PyPaimon source archives are the release. Maven, TestPyPI, and GitHub Actions
checks supplement source verification but do not replace it.

Report only the checks, platforms, and tool versions that you actually used.

## Collect the candidate inputs

Take all values and URLs from the vote email:

```shell
PAIMON_VERSION="2.0.0"
RC_NUMBER="1"
RC_TAG="release-${PAIMON_VERSION}-rc${RC_NUMBER}"

PAIMON_RC_DIR="paimon-${PAIMON_VERSION}-rc${RC_NUMBER}"
PAIMON_ARCHIVE="apache-paimon-${PAIMON_VERSION}-src.tgz"
PYPAIMON_RC_DIR="pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}"
PYPAIMON_ARCHIVE="pypaimon-${PAIMON_VERSION}.tar.gz"
```

Download the candidates from ASF dist dev, not from a third-party mirror:

```shell
curl -O "https://dist.apache.org/repos/dist/dev/paimon/${PAIMON_RC_DIR}/${PAIMON_ARCHIVE}"
curl -O "https://dist.apache.org/repos/dist/dev/paimon/${PAIMON_RC_DIR}/${PAIMON_ARCHIVE}.asc"
curl -O "https://dist.apache.org/repos/dist/dev/paimon/${PAIMON_RC_DIR}/${PAIMON_ARCHIVE}.sha512"

curl -O "https://dist.apache.org/repos/dist/dev/paimon/${PYPAIMON_RC_DIR}/${PYPAIMON_ARCHIVE}"
curl -O "https://dist.apache.org/repos/dist/dev/paimon/${PYPAIMON_RC_DIR}/${PYPAIMON_ARCHIVE}.asc"
curl -O "https://dist.apache.org/repos/dist/dev/paimon/${PYPAIMON_RC_DIR}/${PYPAIMON_ARCHIVE}.sha512"

curl -O https://downloads.apache.org/paimon/KEYS
```

Keep the workflow run URL, announced commit SHA, Java Nexus staging URL, and
TestPyPI URL beside these files. All of them must identify this RC.

## Verify signatures and checksums

```shell
gpg --import KEYS
gpg --verify "${PAIMON_ARCHIVE}.asc" "${PAIMON_ARCHIVE}"
gpg --verify "${PYPAIMON_ARCHIVE}.asc" "${PYPAIMON_ARCHIVE}"
```

Confirm that both signatures are good and that the full signing-key fingerprint
belongs to the RM and appears in the downloaded `KEYS` file. Also inspect the
exact key or subkey that produced each signature:

```shell
gpg --with-subkey-fingerprint \
  --list-keys "<FULL_SIGNING_KEY_FINGERPRINT>"
```

The signing key must be RSA and at least 2048 bits, as required by the
[ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html).
A newly generated release key should be RSA 4096. DSA, ECDSA, EdDSA, and
Ed25519 signing keys do not satisfy this policy, even when the signature is
cryptographically valid.

On Linux:

```shell
sha512sum -c "${PAIMON_ARCHIVE}.sha512"
sha512sum -c "${PYPAIMON_ARCHIVE}.sha512"
```

On macOS:

```shell
shasum -a 512 -c "${PAIMON_ARCHIVE}.sha512"
shasum -a 512 -c "${PYPAIMON_ARCHIVE}.sha512"
```

## Verify Git provenance

```shell
git clone https://github.com/apache/paimon.git paimon-candidate-git
git -C paimon-candidate-git fetch --tags
git -C paimon-candidate-git tag -v "${RC_TAG}"
git -C paimon-candidate-git rev-parse "refs/tags/${RC_TAG}^{commit}"
```

The resolved commit must equal both the SHA in the vote email and the
`head_sha` of the successful Release workflow run. Inspect the commits since the
previous release and confirm that the RC tag was prepared from the intended
release branch. The release branch may advance after the RC is tagged and does
not need to remain equal to the RC commit; verify the immutable tag and candidate
artifacts announced for the vote.

## Inspect the source archives

List the archives before extracting them:

```shell
tar tzf "${PAIMON_ARCHIVE}" | sed -n '1,100p'
tar tzf "${PYPAIMON_ARCHIVE}" | sed -n '1,100p'

# Use Python as well because macOS tar hides AppleDouble (._*) entries.
python3 - "${PAIMON_ARCHIVE}" "${PYPAIMON_ARCHIVE}" <<'PY'
import sys
import tarfile

for archive in sys.argv[1:]:
    with tarfile.open(archive, "r:*") as source:
        for member in source.getmembers():
            parts = [
                part
                for part in member.name.split("/")
                if part not in ("", ".")
            ]
            if member.name.startswith("/") or ".." in parts:
                raise ValueError(
                    "%s contains an unsafe path: %s"
                    % (archive, member.name)
                )
            if "__MACOSX" in parts or any(
                part.startswith("._") for part in parts
            ):
                raise ValueError(
                    "%s contains macOS metadata: %s"
                    % (archive, member.name)
                )
    print("Archive metadata check passed: " + archive)
PY
```

Check at least the following:

- The Paimon archive has exactly one top-level directory named
  `paimon-PAIMON_VERSION`.
- The PyPaimon archive has exactly one top-level directory named
  `pypaimon-PAIMON_VERSION`.
- `LICENSE`, `NOTICE`, README files, build files, dependency declarations, and
  required source files are present and correct.
- No Git metadata, IDE state, credentials, Maven `target` directories, Python
  `dist` or `__pycache__` directories, compiled classes, generated JARs, or
  unrelated binary files are included.
- All bundled third-party material is compatible with the Apache License 2.0
  and is recorded in `LICENSE` or `NOTICE` where required.
- Maven POMs use `PAIMON_VERSION` without `-SNAPSHOT`.
- `paimon-python/setup.py` and PyPaimon package metadata use
  `PAIMON_VERSION` without `.dev`.

Extract the Paimon candidate:

```shell
tar xzf "${PAIMON_ARCHIVE}"
```

Run the repository licensing checks. The PyPaimon checker temporarily extracts
its source package and removes that temporary directory when it finishes:

```shell
cd "paimon-${PAIMON_VERSION}"
mvn -N -ntp -DskipTests apache-rat:check
cd ..

SOURCE_PACKAGE="${PYPAIMON_ARCHIVE}" \
  "./paimon-${PAIMON_VERSION}/paimon-python/dev/check-licensing.sh"
```

Review the generated reports rather than relying only on the exit codes.
After the licensing check, extract a fresh PyPaimon source tree for the build
and test steps below:

```shell
tar xzf "${PYPAIMON_ARCHIVE}"
```

## Build Java from the source archive

Compile the signed source archive and run an appropriate test or smoke test on
at least one supported JDK. Record `java -version`, `mvn -version`, and the
exact scope. Voters do not need to reproduce the complete CI matrix or run any
full JDK test lane locally. The RM is responsible for reviewing the relevant CI
results before starting the vote.

### JDK 8 lane

This lane covers the default reactor, Flink 1.x, and Spark 3.x. As an optional
compatibility check, voters may run the packaging-equivalent build without
reproducing the full JDK 8 test lane:

```shell
(
  cd "paimon-${PAIMON_VERSION}"
  mvn -ntp clean install -DskipTests \
    -Pdocs-and-source,spark3,flink1
)
```

### JDK 11 lane

This lane covers Flink 2.x and Iceberg. As an optional compatibility check,
voters may run the packaging-equivalent build without reproducing the full JDK
11 test lane:

```shell
(
  cd "paimon-${PAIMON_VERSION}"
  mvn -ntp clean install -DskipTests -Pdocs-and-source,flink2 \
    -pl org.apache.paimon:paimon-flink-2.0,org.apache.paimon:paimon-flink-2.1,org.apache.paimon:paimon-flink-2.2,org.apache.paimon:paimon-iceberg \
    -am
)
```

### JDK 17 lane

This lane covers Spark 4.x. As an optional compatibility check, voters may run
the packaging-equivalent build without reproducing the full JDK 17 test lane:

```shell
(
  cd "paimon-${PAIMON_VERSION}"
  mvn -ntp clean install -DskipTests -Pdocs-and-source,spark4 \
    -pl paimon-spark/paimon-spark-4.0,paimon-spark/paimon-spark-4.1,paimon-spark/paimon-spark-4.2 \
    -am
)
```

Investigate warnings and skipped modules. A successful compiler exit does not,
by itself, establish that the candidate is suitable for release. The optional
`-DskipTests` commands do not by themselves satisfy the requirements for a
binding `+1`; report the actual source build, test, or smoke-test scope you
completed. This does not require covering every JDK lane.

## Verify the Java staging repository

Use the exact `orgapachepaimon-XXXX` staging URL from the vote email. Do not
resolve artifacts from Maven Central or a local cache when testing the
candidate.

For the staging repository:

- Confirm that its status is closed, so the candidate cannot change during the
  vote.
- Confirm that the version is `PAIMON_VERSION`, never a snapshot.
- Confirm that each published module has its POM, main artifact, source JAR,
  detached signatures, and checksums where applicable. Confirm Javadoc JARs for
  modules where Maven produces them; Scala-only and wrapper modules may not
  produce one.
- Inspect shaded and bundled JARs for the required `META-INF/LICENSE` and
  `META-INF/NOTICE` content and for unexpected duplicate or bundled
  dependencies.
- Resolve representative artifacts in a clean Maven repository and run a small
  consumer smoke test.

Confirm the lane boundaries:

| Lane | Must contain |
| --- | --- |
| JDK 8 | Default Java 8 artifacts, Flink 1.x, and Spark 3.x |
| JDK 11 | Flink 2.x, `paimon-flink2-common`, and `paimon-iceberg` |
| JDK 17 | Spark 4.x and its Scala 2.13 common artifacts |

Check representative class-file targets. Java class major versions are 52 for
Java 8, 55 for Java 11, and 61 for Java 17:

```shell
JAR_FILE="/path/to/a/representative.jar"
CLASS_NAME="$(jar tf "${JAR_FILE}" | grep '\.class$' | grep -v 'module-info' | head -1 | sed 's#/#.#g;s#\.class$##')"
javap -verbose -classpath "${JAR_FILE}" "${CLASS_NAME}" | grep 'major version'
```

Run this against at least one representative JAR from each lane. Also check
that Java 8 artifacts can be loaded by a JDK 8 runtime; class-file inspection
alone does not detect every use of a newer JDK API.

## Build and test PyPaimon from source

First build the official PyPaimon source candidate in an isolated environment:

```shell
python3 -m venv pypaimon-rc-venv
. pypaimon-rc-venv/bin/activate
python -m pip install --upgrade pip setuptools wheel

cd "pypaimon-${PAIMON_VERSION}"
python setup.py sdist bdist_wheel
python -m pip install "dist/pypaimon-${PAIMON_VERSION}"*.whl
python -c "import pypaimon; print('PyPaimon import OK')"
cd ..
```

Inspect the generated sdist and wheel. They must contain the expected Python
sources, `LICENSE`, `NOTICE`, README, and package metadata, with no tests,
caches, credentials, or unrelated build output.

This guide covers a combined Paimon and PyPaimon release. The complete Python
test suite for the PyPaimon candidate is supplied in the signed Paimon source
archive; the PyPaimon source distribution must not be used by itself for an
independent release. Run the tests from
`paimon-PAIMON_VERSION/paimon-python` on as many supported Python versions as
your environment allows. The project CI selects Python 3.6, 3.7, 3.10, and
3.11 for its main compatibility lanes:

```shell
(
  cd "paimon-${PAIMON_VERSION}/paimon-python"
  python -m pip install -r dev/requirements.txt
  python -m pip install -r dev/requirements-dev.txt
  python -m pytest pypaimon/tests -v
)
```

Some optional integration tests need additional services or packages. Record
which tests ran, skipped, or failed.

## Verify the TestPyPI candidate

Install the exact RC version in a new environment:

```shell
python3 -m venv pypaimon-testpypi-venv
. pypaimon-testpypi-venv/bin/activate
python -m pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  "pypaimon==${PAIMON_VERSION}rc${RC_NUMBER}"
python -c "import pypaimon; print('TestPyPI package OK')"
```

Check the available files, package metadata, Python requirement, dependencies,
license files, and a representative read/write operation. The RC suffix is the
only intended version change from the final PyPaimon source candidate; any
source-code difference is a release blocker.

## Review GitHub Actions evidence

Open the workflow run linked in the vote email and confirm:

- it was triggered from the signed `RC_TAG`;
- `head_sha` equals the announced commit;
- the common validation job confirmed that `RC_TAG` is exactly
  `release-PAIMON_VERSION-rcRC_NUMBER` and that `PAIMON_VERSION` equals the
  root Maven `project.version`;
- the JDK 8, JDK 11, and JDK 17 repository lanes completed successfully;
- the combined Java repository artifact has the expected tag, commit,
  manifest, and SHA-512 checksums and identifies all three lanes;
- each lane's deploy-enabled expected-project inventory matches its POMs, main
  JARs, and source JARs in the combined repository, with Javadoc JARs retained
  where Maven produces them;
- the Python packaging and Python publishing job results are clearly recorded;
- the logs show the expected JDK and Python versions;
- the TestPyPI version equals the version in the vote email;
- no later rerun silently replaced a failed lane with artifacts from another
  commit.

The Java repository image is unsigned workflow output; verify that the closed
Nexus repository contains the same artifacts plus the RM's signatures. The
repository lanes package release artifacts; they are not full JDK test lanes.
Voters do not need to require or reproduce a full test run for any JDK lane.
The RM manually confirms that the relevant CI checks have passed before
starting the vote; unrelated CI failures are assessed separately and are not
an automatic release gate.

## Report your vote

Reply to the vote thread with `+1`, `0`, or `-1`, state whether your vote is
binding, and list the checks you completed.

```text
+1 (binding/non-binding)

Verified:
- signed RC tag and announced commit SHA
- Paimon and PyPaimon GPG signatures, RSA signing-key policy, and SHA-512 checksums
- LICENSE, NOTICE, source-only archive contents, and release versions
- Paimon source build/test scope on <OS/ARCH>, JDK <version>, Maven <version>
- Java staging repository and representative class-file targets
- PyPaimon source build/tests on Python <versions>
- TestPyPI installation and smoke test
- GitHub Actions run provenance, Java package manifests, and Python artifacts
```

For a `-1`, describe the failure precisely enough for the RM to reproduce it.
See [Creating a Release](./creating-a-release.md) for the RM workflow.
