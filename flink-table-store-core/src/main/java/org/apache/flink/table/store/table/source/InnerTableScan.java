package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;

import java.util.List;

public interface InnerTableScan extends TableScan {

    default InnerTableScan withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    InnerTableScan withFilter(Predicate predicate);
}
