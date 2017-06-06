package ca.magenta.yes.data;

import org.slf4j.LoggerFactory;

public class MasterIndexRecord {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MasterIndexRecord.class.getName());

    static final int PARTITION_NAME_MAX_LENGTH = 20;

    private final RuntimeTimestamps runtimeTimestamps;
    private final String longTermIndexName;
    private final String partitionName;

    public MasterIndexRecord(String longTermIndexName, String partitionName, RuntimeTimestamps runtimeTimestamps) {
        this.longTermIndexName = longTermIndexName;
        this.partitionName = partitionName;
        this.runtimeTimestamps = runtimeTimestamps;
    }

    @Override
    public String toString() {
        return "MasterIndexRecord{" +
                "partition='" + partitionName +
                "', longTermIndexName='" + longTermIndexName +
                "', olderSrc=" + runtimeTimestamps.getOlderSrcTimestamp() +
                ", newerSrc=" + runtimeTimestamps.getNewerSrcTimestamp() +
                ", olderRx=" + runtimeTimestamps.getOlderRxTimestamp() +
                ", newerRx=" + runtimeTimestamps.getNewerRxTimestamp() +
                ", runStart=" + runtimeTimestamps.getRunStartTimestamp() +
                ", runEnd=" + runtimeTimestamps.getRunEndTimestamp() +
                '}';
    }

    String getLongTermIndexName() {
        return longTermIndexName;
    }

    RuntimeTimestamps getRuntimeTimestamps() {
        return runtimeTimestamps;
    }

    String getPartitionName() {
        return partitionName;
    }

    public static class RuntimeTimestamps {

        private long olderSrcTimestamp;
        private long newerSrcTimestamp;

        private long olderRxTimestamp;
        private long newerRxTimestamp;

        private final long runStartTimestamp;
        private long runEndTimestamp;

        public RuntimeTimestamps() {
            runStartTimestamp = System.currentTimeMillis();

            olderSrcTimestamp = Long.MAX_VALUE;
            newerSrcTimestamp = 0;

            olderRxTimestamp = Long.MAX_VALUE;
            newerRxTimestamp = 0;
        }

        RuntimeTimestamps(long olderSrcTimestamp,
                          long newerSrcTimestamp,
                          long olderRxTimestamp,
                          long newerRxTimestamp,
                          long runStartTimestamp,
                          long runEndTimestamp) {
            this.olderSrcTimestamp = olderSrcTimestamp;
            this.newerSrcTimestamp = newerSrcTimestamp;
            this.olderRxTimestamp = olderRxTimestamp;
            this.newerRxTimestamp = newerRxTimestamp;
            this.runStartTimestamp = runStartTimestamp;
            this.runEndTimestamp = runEndTimestamp;
        }

        public void compute(long srcTimestamp, long rxTimestamp) {

            if (srcTimestamp < olderSrcTimestamp)
                olderSrcTimestamp = srcTimestamp;
            if (srcTimestamp > newerSrcTimestamp)
                newerSrcTimestamp = srcTimestamp;

            if (rxTimestamp < olderRxTimestamp)
                olderRxTimestamp = rxTimestamp;
            if (rxTimestamp > newerRxTimestamp)
                newerRxTimestamp = rxTimestamp;
        }

        long getOlderSrcTimestamp() {
            return olderSrcTimestamp;
        }

        long getNewerSrcTimestamp() {
            return newerSrcTimestamp;
        }

        long getOlderRxTimestamp() {
            return olderRxTimestamp;
        }

        long getNewerRxTimestamp() {
            return newerRxTimestamp;
        }

        long getRunStartTimestamp() {
            return runStartTimestamp;
        }

        long getRunEndTimestamp() {
            return runEndTimestamp;
        }

        public void setRunEndTimestamp(long runEndTimestamp) {
            this.runEndTimestamp = runEndTimestamp;
        }
    }


}
