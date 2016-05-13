package com.github.tbr.kafka.util;

public final class ClusterConstants {
	public static final String DEFAULT_HOST_NAME = "localhost";
	public static final int DEFAULT_ZK_PORT = 2181;
	public static final int DEFAULT_ZK_TICK_TIME = 2000;
	public static final int DEFAULT_ZK_CONNECTION_TIMEOUT=30000;
	public static final int DEFAULT_ZK_MAX_CLENT_CNXNS = 1000;
	public static final String DEFAULT_ZK_SNAPSHOT_DIR = "zk_snapshots";
	public static final String DEFAULT_ZK_LOG_DIR = "zk_logs";
	public static final String KAFKA_ZK_CONNECT = "zookeeper.connect";
	public static final String KAFKA_BROKER_ID = "broker.id";
	public static final String KAFKA_HOST_NAME = "host.name";
	public static final String KAFKA_PORT = "port";
	public static final String KAFKA_LOG_DIR = "log.dir";
}
