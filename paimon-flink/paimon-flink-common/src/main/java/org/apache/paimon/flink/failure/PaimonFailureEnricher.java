package org.apache.paimon.flink.failure;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.core.failure.FailureEnricher;

public class PaimonFailureEnricher implements FailureEnricher {

    public PaimonFailureEnricher(){
    }

    @Override
    public Set<String> getOutputKeys() {
        return null;
    }

    @Override
    public CompletableFuture<Map<String, String>> processFailure(Throwable cause, Context context) {
        try (FileWriter writer = new FileWriter("/Users/zhuoyuchen/Documents/GitHub/incubator-paimon/data/log/error.log", true)) {
            writer.write("Error occurred: " + cause.getMessage() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }
}
