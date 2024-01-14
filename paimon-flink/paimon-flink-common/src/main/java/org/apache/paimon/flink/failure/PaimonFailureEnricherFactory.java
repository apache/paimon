package org.apache.paimon.flink.failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricherFactory;

public class PaimonFailureEnricherFactory implements FailureEnricherFactory {
    @Override
    public FailureEnricher createFailureEnricher(Configuration conf) {
        return new PaimonFailureEnricher();
    }
}
