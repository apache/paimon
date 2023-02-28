package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;

import java.util.Arrays;
import java.util.List;

public interface InnerTableRead extends TableRead {

    default InnerTableRead withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    InnerTableRead withFilter(Predicate predicate);

    default InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        int[][] nestedProjection =
                Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        return withProjection(nestedProjection);
    }

    InnerTableRead withProjection(int[][] projection);
}
