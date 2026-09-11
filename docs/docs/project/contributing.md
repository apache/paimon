---
title: "Contributing"
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

# Contributing

Apache Paimon welcomes contributions of code, documentation, testing, reviews,
and community support. Start with a task below, and discuss questions in the
relevant issue or on the [mailing lists](https://github.com/apache/paimon#mailing-lists).

## What do you want to do?

| Contribution | How to start |
| --- | --- |
| Ask or answer a question | Use the [user mailing list](https://github.com/apache/paimon#mailing-lists), or help users in existing issues. |
| Report a bug | Open a [GitHub issue](https://github.com/apache/paimon/issues) with the Paimon and engine versions, expected and actual behavior, and a minimal reproduction. Remove credentials from logs and configuration. |
| Report a possible vulnerability | Follow the private [security reporting process](./security.md#reporting-security-issues). |
| Propose a feature | Explain the use case and design in an issue or on `dev@paimon.apache.org` before implementation. |
| Contribute code | Follow the [code contribution guide](#code-contribution-guide). |
| Improve documentation | Describe what readers need, update the relevant page, and [build the documentation](#documentation-changes). |
| Review code | Use the [code review guide](#code-review-guide). |
| Test a release candidate | Follow the [verification guide](./verifying-a-release-candidate.md) and report the checks you completed. |
| Help the community grow | Write about Paimon, organize or attend a meetup, and share useful material on `dev@paimon.apache.org`. |

## Code Contribution Guide

![Contribution workflow: discuss the problem and agree on an approach, implement a focused change, address review feedback, and let a committer merge it.](/img/project/contribution-workflow.svg)

[Open the contribution diagram at full size](/img/project/contribution-workflow.svg).

### Discuss {#consensus}

Create an issue or mailing-list discussion that explains the problem and your
proposed approach. Reach agreement before making a substantial change.

When requesting assignment to an issue, explain your understanding, design,
and, where useful, a proof of concept. An assignment request alone does not
provide enough information. If you are working on an issue, wait until it is
assigned to you before opening the pull request.

### Implement {#implement}

1. Implement the approach agreed in the discussion. Keep the change focused and
   avoid unrelated refactoring or formatting.
2. Add tests that exercise the changed behavior, including a regression test
   for a bug fix. Update documentation for user-visible behavior.
3. Follow the repository's [build instructions](https://github.com/apache/paimon#building).
   Run the relevant tests and formatting checks, and enable GitHub Actions in
   your fork.
4. Open a pull request describing the problem, resulting behavior, and
   validation. Link the issue, if there is one; use `Fixes #123` when the pull
   request resolves it.

### Review {#review}

Work with reviewers and explain how each concern was addressed. Keep tests
passing as the change evolves. Leave review conversations open for reviewers
to resolve after they check your response.

If implementation reveals a need to change the agreed approach, explain the
new design before expanding the pull request.

### Merge {#merge}

A Paimon committer checks that the contribution meets the project requirements
and merges it after review. A pull request with passing tests still needs
review and agreement on its behavior.

## Documentation changes

The documentation source is in `docs/docs`, static images are in
`docs/static/img`, and navigation is defined explicitly in `docs/sidebars.js`.
When adding a page, add its document ID to the appropriate sidebar category.
Use relative Markdown links between pages so that links remain within the
selected documentation version.

Prefer editable SVG for diagrams. Give each diagram descriptive alternative
text and explain its essential steps in the page so that the information is
also available without the image.

From the repository root, install the documentation dependencies and build the
site:

```shell
cd docs
yarn install
yarn build
```

The build runs the REST OpenAPI contract checks and compiles the documentation.
Review the output for broken links and inspect the changed pages with
`yarn serve`, including images and narrow-screen layouts. See the
[documentation README](https://github.com/apache/paimon/blob/master/docs/README.md)
for local development and generated configuration tables.

## Code Review Guide

Review these four areas in order. Establish the purpose and agreement on the
change before spending time on implementation details.

### 1. Is the contribution well described?

The description should make the problem and resulting behavior clear. Small
fixes need only a short explanation. Link prior issue or mailing-list
discussions when the implementation follows an agreed design; explain any
departures from that design.

### 2. Does it need attention from specific committers?

Some changes need review from people familiar with the affected component or
contract. When specific attention is required, one of the tagged committers
or contributors should give the final approval.

### 3. Does the implementation meet Paimon's quality standards?

- Check correctness, robustness, maintainability, and testability.
- Consider performance when changing a performance-sensitive path.
- Check that tests cover the changed behavior and run efficiently.
- When dependencies change, check whether `LICENSE` or `NOTICE` needs updating.

Refer to the [Flink Java Code Style and Quality Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-java/)
for code guidelines.

### 4. Is the documentation current?

Document new features and changes to configuration, public APIs, or observable
behavior. Check examples and links, and ensure that the documentation build
passes.

## Become a Committer

Sustained code and community contributions can lead to nomination as a
committer. See the [committer guide](./committer.md) for the nomination process
and community expectations.
