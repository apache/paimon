package org.apache.paimon.presto;

import org.apache.paimon.table.source.Split;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import java.util.List;

/** Trino {@link ConnectorSplit}. */
public class PrestoSplit extends PrestoSplitBase {

    @JsonCreator
    public PrestoSplit(@JsonProperty("splitSerialized") String splitSerialized) {
        super(splitSerialized);
    }

    public static PrestoSplit fromSplit(Split split) {
        return new PrestoSplit(EncodingUtils.encodeObjectToString(split));
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
        return ImmutableList.of();
    }
}
