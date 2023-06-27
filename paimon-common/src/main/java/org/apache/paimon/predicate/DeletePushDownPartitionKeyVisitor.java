package org.apache.paimon.predicate;

import java.util.List;

public class DeletePushDownPartitionKeyVisitor implements FunctionVisitor<Boolean> {

    private final List<String> partitionKeys;

    public DeletePushDownPartitionKeyVisitor(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    @Override
    public Boolean visitIsNotNull(FieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitIsNull(FieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitEqual(FieldRef fieldRef, Object literal) {
        return partitionKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitAnd(List<Boolean> children) {
        return children.stream().reduce((first, second) -> first && second).get();
    }

    @Override
    public Boolean visitOr(List<Boolean> children) {
        return false;
    }
}
