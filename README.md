# Paimon (Incubating)

Paimon is a streaming data lake platform that supports high-speed data ingestion, change data tracking and efficient real-time analytics.

Background and documentation is available at https://paimon.apache.org

Paimon's former name was Flink Table Store, developed from the Flink community. The architecture refers to some design concepts of Iceberg.
Thanks to Apache Flink and Apache Iceberg.

## Collaboration

Paimon tracks issues in GitHub and prefers to receive contributions as pull requests.

## Mailing Lists

<table class="table table-striped">
  <thead>
    <th class="text-center">Name</th>
    <th class="text-center">Subscribe</th>
    <th class="text-center">Digest</th>
    <th class="text-center">Unsubscribe</th>
    <th class="text-center">Post</th>
    <th class="text-center">Archive</th>
  </thead>
  <tr>
    <td>
      <strong>user</strong>@paimon.apache.org<br>
      <small>User support and questions mailing list</small>
    </td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:user-subscribe@paimon.apache.org">Subscribe</a></td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:user-digest-subscribe@paimon.apache.org">Subscribe</a></td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:user-unsubscribe@paimon.apache.org">Unsubscribe</a></td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:user@paimon.apache.org">Post</a></td>
    <td class="text-center">
      <a href="https://lists.apache.org/list.html?user@paimon.apache.org">Archives</a>
    </td>
  </tr>
  <tr>
    <td>
      <strong>dev</strong>@paimon.apache.org<br>
      <small>Development related discussions</small>
    </td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:dev-subscribe@paimon.apache.org">Subscribe</a></td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:dev-digest-subscribe@paimon.apache.org">Subscribe</a></td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:dev-unsubscribe@paimon.apache.org">Unsubscribe</a></td>
    <td class="text-center"><i class="fa fa-pencil-square-o"></i> <a href="mailto:dev@paimon.apache.org">Post</a></td>
    <td class="text-center">
      <a href="https://lists.apache.org/list.html?dev@paimon.apache.org">Archives</a>
    </td>
  </tr>
</table>

<b style="color:red">Please make sure you are subscribed to the mailing list you are posting to!</b> If you are not subscribed to the mailing list, your message will either be rejected (dev@ list) or you won't receive the response (user@ list).

## Building
JDK 8/11 is required for building the project.

- Run the `mvn clean package -DskipTests` command to build the project.
- Run the `mvn spotless:apply` to format the project (both Java and Scala).

## License

The code in this repository is licensed under the [Apache Software License 2](LICENSE).
