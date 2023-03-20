# Paimon (Incubating)

Paimon is a streaming data lake platform that supports high-speed data ingestion, change data tracking and efficient real-time analytics.

Background and documentation is available at https://paimon.apache.org

Paimon's former name was Flink Table Store, developed from the Flink community. The architecture refers to some design concepts of Iceberg.
Thanks to Apache Flink and Apache Iceberg.

## Collaboration

Paimon tracks issues in GitHub and prefers to receive contributions as pull requests.

Community discussions happen primarily on the [dev mailing list](mailto:dev@paimon.apache.org) or on specific issues.

## Building

- Run the `mvn clean package -DskipTests` command to build the project.
- Run the `mvn spotless:apply` to format the project (both Java and Scala).

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
