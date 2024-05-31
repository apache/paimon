package org.apache.paimon.flink;

import org.apache.paimon.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.function.BinaryOperator;

public class AskwangJavaITCase {

  @Test
  public void testBinaryOperator() {
    BinaryOperator<Integer> binaryOperator1 = (m, n) -> m + n;
    BinaryOperator<Integer> binaryOperator2 = Integer::sum;
    Integer sum = binaryOperator2.apply(3, 5);
    assert(sum == 8);
  }

  @Test
  public void testPathGetName() {
    String SNAPSHOT_PREFIX = "snapshot-";
    Path path = new Path("hdfs://ns/warehouse/db/tb/snapshot/snapshot-1");
    String name = path.getName();
    System.out.println(name);
    String index = name.substring("snapshot-".length());
    System.out.println(index);

    Path path1 = new Path("hdfs://ns/warehouse/db/tb/snapshot/EARLIEST");
    // path_length < begin_index => throw StringIndexOutOfBoundsException
    String pos = path1.getName().substring(SNAPSHOT_PREFIX.length());
    System.out.println(pos);
  }
}
