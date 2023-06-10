package org.apache.paimon.format;

import java.util.HashSet;
import java.util.Set;

/** the helper class for skip the statistics collect. */
public class ColumnStatisticsCollectSkipper {
    private Set<Integer> skipColumnIndexes = new HashSet<>();

    public boolean skipStatistics(int index) {
        return skipColumnIndexes.contains(index);
    }

    public void addSkipColumnIndex(int index) {
        skipColumnIndexes.add(index);
    }
}
