---
title: "Publishing a Release"
sidebar_position: 4
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

# Publishing a Release

This guide is for the Release Manager after the combined Paimon and PyPaimon
vote has passed and its result has been recorded. To prepare a candidate, start
with [Creating a Release](./creating-a-release.md).

## Confirm the approved inputs

Use the exact candidate named in the successful vote. Keep the vote result,
source URLs, closed Nexus repository ID, commit SHA, and workflow run URL
available throughout publication.

Set the same variables used when preparing that candidate. These values are
examples; replace them with the approved release values:

```shell
PAIMON_VERSION="2.0.0"
DOC_VERSION="2.0"
RC_NUMBER="1"
RELEASE_BRANCH="release-2.0"
RC_REF="release-${PAIMON_VERSION}-rc${RC_NUMBER}"
RELEASE_TAG="release-${PAIMON_VERSION}"
```

Run the tag commands in the release clone with the RM's signing key configured
and the approved RC tag available locally. Use the same ASF distribution and
Nexus access configured during [RM setup](./creating-a-release.md#one-time-rm-setup).

| Publish | Preserve |
| --- | --- |
| Final Git tag | The exact approved RC commit |
| ASF source releases | The approved archives, signatures, and checksums |
| Maven Central artifacts | The exact closed Nexus repository approved by the vote |
| PyPI package | The approved source commit, using the final Python version |
| Documentation and website | Version labels, links, and examples matching the release |

## Create the final signed tag

The final tag must point to exactly the approved RC commit:

```shell
git tag -s "${RELEASE_TAG}" "refs/tags/${RC_REF}^{commit}" \
  -m "Release Apache Paimon ${PAIMON_VERSION}"

test "$(git rev-parse "refs/tags/${RC_REF}^{commit}")" = \
     "$(git rev-parse "refs/tags/${RELEASE_TAG}^{commit}")"
git tag -v "${RELEASE_TAG}"
git push origin "refs/tags/${RELEASE_TAG}:refs/tags/${RELEASE_TAG}"
```

## Promote the source releases

Move, rather than copy or rebuild, both approved candidate directories:

```shell
svn mv -m "Release Apache Paimon ${PAIMON_VERSION}" \
  "https://dist.apache.org/repos/dist/dev/paimon/paimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "https://dist.apache.org/repos/dist/release/paimon/paimon-${PAIMON_VERSION}"

svn mv -m "Release PyPaimon ${PAIMON_VERSION}" \
  "https://dist.apache.org/repos/dist/dev/paimon/pypaimon-${PAIMON_VERSION}-rc${RC_NUMBER}" \
  "https://dist.apache.org/repos/dist/release/paimon/pypaimon-${PAIMON_VERSION}"
```

## Promote convenience artifacts

1. In Nexus, confirm that the recorded Java staging repository is still closed
   and has the exact artifact tree approved by the vote.
2. Release that exact closed repository to Maven Central. Do not upload or
   rebuild the Java artifacts again.
3. Confirm that the final tag's PyPI publish job builds
   `pypaimon==PAIMON_VERSION` from the approved tag commit and does not change
   project source.
4. Verify Maven Central and PyPI before announcing the release.

## Publish and announce

Create a GitHub release from `release-PAIMON_VERSION`, review the generated
notes, and link both source releases.

Before announcing the release, publish the versioned documentation and update
the project website. Treat the documentation in `apache/paimon` and the project
website in `apache/paimon-website` as two separate, required updates.

1. In `apache/paimon`, publish documentation for `DOC_VERSION` from the release
   branch so that the published content matches the released code:
   - On `RELEASE_BRANCH`, update `docs/docusaurus.config.js` with the released
     `baseUrl`, `version`, `versionTitle`, `branch`, `editUrl`, `isStable`,
     `stableDocs`, `previousDocs`, and navbar version menu.
   - On `master`, set the next development version and update `stableDocs`,
     `previousDocs`, and the navbar version menu to include `DOC_VERSION` as
     the stable release.
   - Review `docs/docs/project/download.mdx` and any release-specific engine or
     compatibility information. The `@@VERSION@@`, `<Stable>`, and `<Unstable>`
     sections must render the released artifacts on the stable site.
   - Run `yarn build` from the `docs` directory for both configurations.
2. In `apache/paimon-website`, update every public release entry point:
   - Add the Paimon and PyPaimon source archives, checksums, signatures, and
     current dependency examples to `community/docs/downloads.md`.
   - Create or update the appropriate
     `community/docs/releases/release-${DOC_VERSION}.md` release note. Its
     `version` front matter must equal `PAIMON_VERSION`, and its weight must
     place it correctly in the release list.
   - Add `DOC_VERSION` to the `versions` list in
     `src/app/components/header/header.component.ts`. If the menu keeps a fixed
     number of versions, remove the oldest entry.
   - Run `pnpm build` to parse the release metadata and build the website.
3. After deployment, verify all public entry points before sending the
   announcement:
   - `https://paimon.apache.org/docs/${DOC_VERSION}/` serves the released docs
     and the version switcher identifies it as stable;
   - the homepage `DOCUMENT` menu includes `DOC_VERSION` on desktop and mobile;
   - `https://paimon.apache.org/downloads/` lists both signed source releases;
   - `https://paimon.apache.org/releases/${PAIMON_VERSION}` shows the release
     note.

After ASF mirrors, Maven Central, PyPI, the versioned documentation, and the
project website are all available, announce the release to
`dev@paimon.apache.org` and `announce@apache.org` from an `@apache.org` address.

Remove superseded releases from the live ASF distribution area when required;
they remain available from the
[Apache archive](https://archive.apache.org/dist/paimon/).

## Completion checklist

- The final signed tag resolves to the approved RC commit.
- Both source releases and their signatures and checksums are available.
- Maven Central and PyPI expose the intended release versions.
- Versioned documentation, download links, and release notes are published.
- The announcement links the public release, and superseded candidates have
  been handled without replacing the artifacts reviewed in the vote.
