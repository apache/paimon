package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.util.Optional;

public class PostgresSyncDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "postgres_sync_database";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }


    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        return Optional.empty();
    }

    @Override
    public void printHelp() {

    }
}
