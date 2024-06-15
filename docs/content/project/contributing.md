---
title: "Contributing"
weight: 3
type: docs
aliases:
- /project/contributing.html
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

Apache Paimon is developed by an open and friendly community. Everybody is cordially welcome to join
the community and contribute to Apache Paimon. There are several ways to interact with the community and contribute
to Paimon including asking questions, filing bug reports, proposing new features, joining discussions on the mailing
lists, contributing code or documentation, improving website, testing release candidates and writing corresponding blog etc.

<h1>What do you want to do?</h1>
<p>Contributing to Apache Paimon goes beyond writing code for the project. Below, we list different opportunities to help the project:</p>

<table class="table table-bordered">
  <thead>
    <tr>
      <th>Area</th>
      <th>Further information</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><span class="glyphicon glyphicon-exclamation-sign" aria-hidden="true"></span> Report Bug</td>
      <td>To report a problem with Paimon, open <a href="https://github.com/apache/paimon/issues">Paimon’s issues</a>. <br/>
      Please give detailed information about the problem you encountered and, if possible, add a description that helps to reproduce the problem.</td>
    </tr>
    <tr>
      <td><span class="glyphicon glyphicon-console" aria-hidden="true"></span> Contribute Code</td>
      <td>Read the <a href="#code-contribution-guide">Code Contribution Guide</a></td>
    </tr>
    <tr>
      <td><span class="glyphicon glyphicon-ok" aria-hidden="true"></span> Code Reviews</td>
      <td>Read the <a href="#code-review-guide">Code Review Guide</a></td>
    </tr>
    <tr>
      <td><span class="glyphicon glyphicon-thumbs-up" aria-hidden="true"></span> Release Version</td>
      <td>Releasing a new Paimon version.</td>
    </tr>
    <tr>
      <td><span class="glyphicon glyphicon-user" aria-hidden="true"></span> Support Users</td>
      <td>Reply to questions on the <a href="https://github.com/apache/paimon#mailing-lists">user mailing list</a>,
          check the latest issues in <a href="https://github.com/apache/paimon/issues">Issues</a> for tickets which are actually user questions.
      </td>
    </tr>
    <tr>
      <td><span class="glyphicon glyphicon-volume-up" aria-hidden="true"></span> Spread the Word About Paimon</td>
      <td>Organize or attend a Paimon Meetup, contribute to the Paimon blog, share your conference, meetup or blog
          post on the <a href="https://github.com/apache/paimon#mailing-lists">dev@paimon.apache.org mailing list</a>.
      </td>
    </tr>
    <tr>
      <td colspan="2">
        <span class="glyphicon glyphicon-question-sign" aria-hidden="true"></span> Any other question? Reach out to the
                     <a href="https://github.com/apache/paimon#mailing-lists">dev@paimon.apache.org mailing list</a> to get help!
      </td>
    </tr>
  </tbody>
</table>

## Code Contribution Guide

Apache Paimon is maintained, improved, and extended by code contributions of volunteers. We welcome contributions to Paimon.

Please feel free to ask questions at any time. Either send a mail to the Dev mailing list or comment on the issue you are working on.

<style>
.contribute-grid {
  margin-bottom: 10px;
  display: flex;
  flex-direction: column;
  margin-left: -2px;
  margin-right: -2px;
}

.contribute-grid .column {
  margin-top: 4px;
  padding: 0 2px;
}

@media only screen and (min-width: 480px) {
  .contribute-grid {
    flex-direction: row;
    flex-wrap: wrap;
  }

  .contribute-grid .column {
    flex: 0 0 50%;
  }

  .contribute-grid .column {
    margin-top: 4px;
  }
}

@media only screen and (min-width: 960px) {
  .contribute-grid {
    flex-wrap: nowrap;
  }

  .contribute-grid .column {
    flex: 0 0 25%;
  }
}

.contribute-grid .panel {
  height: 100%;
  margin: 0;
}

.contribute-grid .panel-body {
  padding: 10px;
}

.contribute-grid h2 {
  margin: 0 0 10px 0;
  padding: 0;
  display: flex;
  align-items: flex-start;
}

.contribute-grid .number {
  margin-right: 0.25em;
  font-size: 1.5em;
  line-height: 0.9;
}
</style>

<div class="contribute-grid">
  <div class="column">
    <div class="panel panel-default">
      <div class="panel-body">
        <h2><span class="number">1</span><a href="#consensus">Discuss</a></h2>
        <p>Create an Issue or mailing list discussion and reach consensus</p>
        <p><b>To request an issue, please note that it is not just a "please assign it to me", you need to explain your understanding of the issue, and your design, and if possible, you need to provide your POC code.</b></p>
      </div>
    </div>
  </div>
  <div class="column">
    <div class="panel panel-default">
      <div class="panel-body">
        <h2><span class="number">2</span><a href="#implement">Implement</a></h2>
        <p>Create the Pull Request and the approach agreed upon in the issue.</p>
        <p><b>1.Only create the PR if you are assigned to the issue. 2.Please associate an issue (if any), e.g. fix #123. 3.Please enable the actions of your own clone project.</b></p>
      </div>
    </div>
  </div>
  <div class="column">
    <div class="panel panel-default">
      <div class="panel-body">
        <h2><span class="number">3</span><a href="#review">Review</a></h2>
        <p>Work with the reviewer.</p><br />
        <p><b>1.Make sure no unrelated or unnecessary reformatting changes are included. 2.Please ensure that the test passing. 3.Please don't resolve conversation.</b></p>
      </div>
    </div>
  </div>
  <div class="column">
    <div class="panel panel-default">
      <div class="panel-body">
        <h2><span class="number">4</span><a href="#merge">Merge</a></h2>
        <p>A committer of Paimon checks if the contribution fulfills the requirements and merges the code to the codebase.</p>
      </div>
    </div>
  </div>
</div>

## Code Review Guide

Every review needs to check the following six aspects. **We encourage to check these aspects in order, to avoid
spending time on detailed code quality reviews when formal requirements are not met or there is no consensus in
the community to accept the change.**

#### 1. Is the Contribution Well-Described?

Check whether the contribution is sufficiently well-described to support a good review. Trivial changes and fixes
do not need a long description. If the implementation is exactly according to a prior discussion on issue or the
development mailing list, only a short reference to that discussion is needed.

If the implementation is different from the agreed approach in the consensus discussion, a detailed description of
the implementation is required for any further review of the contribution.

#### 2. Does the Contribution Need Attention from some Specific Committers?

Some changes require attention and approval from specific committers.

If the pull request needs specific attention, one of the tagged committers/contributors should give the final approval.

#### 3. Is the Overall Code Quality Good, Meeting Standard we Want to Maintain in Paimon?

- Does the code follow the right software engineering practices? Is the code correct, robust, maintainable, testable?
- Are the changes performance aware, when changing a performance sensitive part?
- Are the changes sufficiently covered by tests? Are the tests executing fast?
- If dependencies have been changed, were the NOTICE files updated?

Code guidelines can be found in the [Flink Java Code Style and Quality Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-java/).

#### 4. Are the documentation updated?

If the pull request introduces a new feature, the feature should be documented.

## Become a Committer

#### How to become a committer

There is no strict protocol for becoming a committer. Candidates for new committers are typically people that are
active contributors and community members. Candidates are suggested by current committers or PPMC members, and
voted upon by the PPMC.

If you would like to become a committer, you should engage with the community and start contributing to Apache Paimon in
any of the above ways. You might also want to talk to other committers and ask for their advice and guidance.

- Community contributions include helping to answer user questions on the mailing list, verifying release candidates,
  giving talks, organizing community events, and other forms of evangelism and community building. The "Apache Way" has
  a strong focus on the project community, and committers can be recognized for outstanding community contributions even
  without any code contributions.

- Code/technology contributions include contributed pull requests (patches), design discussions, reviews, testing, 
  and other help in identifying and fixing bugs. Especially constructive and high quality design discussions, as well
  as helping other contributors, are strong indicators.

#### Identify promising candidates

While the prior points give ways to identify promising candidates, the following are "must haves" for any committer candidate:

- Being community minded: The candidate understands the meritocratic principles of community management. They do not
  always optimize for as much as possible personal contribution, but will help and empower others where it makes sense.

- We trust that a committer candidate will use their write access to the repositories responsibly, and if in doubt,
  conservatively. It is important that committers are aware of what they know and what they don't know. In doubt, 
  committers should ask for a second pair of eyes rather than commit to parts that they are not well familiar with.

- They have shown to be respectful towards other community members and constructive in discussions.
