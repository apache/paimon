# Continuous integration

`workflows/ci.yml` is the entry point for pull requests, pushes to any branch or
tag, and manual full runs. Contributors can validate changes in their own forks
before opening an upstream PR, once GitHub Actions is enabled in the fork.
Both push and PR runs select tests by changed paths. Updating a PR cancels its
previous CI run; push and publishing runs are not cancelled in progress.

The entry point calls `ci-java.yml`, `ci-python.yml`, `ci-docs.yml` and
`ci-licensing.yml`. `publish-snapshot*.yml`, `release*.yml` and
`maintenance-stale-pr.yml` have separate publishing or maintenance triggers.

## Selecting tests

`tools/ci/plan.py` is the single source of path routing and the Java matrix.
Its selection is shown in the workflow run summary. It uses the PR merge-base
diff, or the entire before/after push diff. Both sides of a rename are included.
Diffs are read from Git without a changed-file API limit. If a complete diff is
unavailable (including a new branch), CI runs all suites. Manual dispatch also
runs everything.

PRs check changed files against a 1 MiB size limit. When the PR diff is unavailable,
this check is skipped with a warning in the log and run summary so that existing
oversized files do not block the full CI run.

| Changed paths | Java tests |
| --- | --- |
| `paimon-spark/**` | Spark 3/4, core and integrations, Flink 1/Spark E2E |
| `paimon-flink/**` | Flink 1/2, core and integrations, both E2E groups, Iceberg GA |
| `paimon-e2e-tests/**` | Both E2E groups |
| `paimon-eslib/**` | ESLib |
| `paimon-iceberg/**` | Core and integrations, Iceberg GA |
| `paimon-full-text/**` | Core and integrations, full text |
| Shared modules, root/build configuration, unclassified source paths | All Java groups |

Java changes retain the artifact licensing check. Changes to `docs/**` build the
website; other Markdown-only changes skip expensive jobs. Changes to
`paimon-python/**` and Java/Python `JavaPy*` test fixtures run Python checks.
Root POM changes also run Python to validate its Java build. Mixed changes take
the union of all required groups. Changes to CI routing or execution scripts
validate every consumer, including Python and documentation.

The core and integrations jobs deliberately still run for Spark/Flink changes:
`paimon-spark-common` owns tests outside the Spark connector test jobs,
`paimon-docs` tests engine configuration documentation, and Hive/Iceberg tests
depend on Flink. Do not remove these jobs by looking only at the workflow name.
`SparkE2eTest` runs in the JDK 8 E2E job and is disabled on JDK 11.

## Builds and caches

`tools/ci/run-java-tests.sh` owns Java build/test commands. Each group installs
its selected modules and their dependencies with `-pl ... -am -DskipTests`, then
tests only its modules. The test invocation does not clean away the first build
or run both `test` and `verify`. Normal Maven checks remain enabled.

Java dependency caches are isolated by suite, JDK and Scala. This prevents a
small group's cache from permanently starving other groups of their dependencies.
Failed Java jobs upload Surefire reports with a seven-day retention period.
Python builds only the Java modules used by the mixed-language tests and their
dependencies; its Python version and compatibility coverage is preserved.

## Required checks and maintenance

Configure branch protection to require **CI result** when adopting this layout.
Remove any requirements referring to the retired individual workflow/job names.
The result job always runs and requires the planner and every selected group to
succeed. Unselected groups must be skipped. A planner failure, failed test,
unexpected skip or cancellation cannot produce a green result.

Every run validates the workflow syntax with pinned actionlint 1.7.11 and runs
the routing/command tests before starting expensive jobs. Locally:

```sh
python3 -m unittest discover -s tools/ci -p 'test_*.py'
actionlint -shellcheck= -pyflakes=
CI_DRY_RUN=true bash tools/ci/run-java-tests.sh spark3 2.12 8
```

When adding an engine version, update both the matrix in `plan.py` and the
module lists in `run-java-tests.sh`, including the core exclusions where needed.
Add behavioral tests for any new routing boundary and retain shared-dependency
fallbacks. Use a manual CI run to validate the complete matrix before adopting
changes to build profiles or test ownership.
