package com.github.tbr.kafka;

import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_HOST_NAME;
import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_ZK_LOG_DIR;
import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_ZK_MAX_CLENT_CNXNS;
import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_ZK_PORT;
import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_ZK_SNAPSHOT_DIR;
import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_ZK_TICK_TIME;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniZooKeeperStandalone {
	private static final Logger LOG = LoggerFactory.getLogger(MiniZooKeeperStandalone.class);
	private final String hostName;
	private final int port;
	private final int tickTime;
	private final File baseDir;
	private final File snapshotDir;
	private final File logDir;
	private ServerCnxnFactory factory;

	public MiniZooKeeperStandalone(File baseDir) {
		this(baseDir, DEFAULT_HOST_NAME, DEFAULT_ZK_PORT, DEFAULT_ZK_TICK_TIME);
	}

	public MiniZooKeeperStandalone(File baseDir, String hostName, int port, int tickTime) {
		this.baseDir = baseDir;
		this.hostName = hostName;
		this.port = port;
		this.tickTime = tickTime;
		this.logDir = new File(baseDir, DEFAULT_ZK_LOG_DIR);
		this.snapshotDir = new File(baseDir, DEFAULT_ZK_SNAPSHOT_DIR);
		validate(logDir);
		validate(snapshotDir);
	}

	private void validate(File dir) {
		if (dir.exists()) {
			if (dir.isDirectory()) {
				try {
					FileUtils.cleanDirectory(dir);
				} catch (IOException e) {
					LOG.error("Could not clean directory : {}", dir);
					throw new RuntimeException(e);
				}
			} else {
				try {
					FileUtils.forceDelete(dir);
				} catch (IOException e) {
					LOG.error("Could not delete file : {}", dir);
					throw new RuntimeException();
				}
			}
		} else {
			try {
				FileUtils.forceMkdir(dir);
			} catch (IOException e) {
				LOG.error("Could not create directory : {}", dir);
			}
		}
	}

	public void startup() {
		LOG.info("Starting zookeeper standalone server...");
		try {
			ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
			this.factory = ServerCnxnFactory.createFactory();
			this.factory.configure(new InetSocketAddress(hostName, port), DEFAULT_ZK_MAX_CLENT_CNXNS);
			this.factory.startup(zkServer);
		} catch (IOException e) {
			LOG.error("Could not start zookeeper server");
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			LOG.error("Zookeeper server interrupted");
			Thread.currentThread().interrupt();
		}
		LOG.info("Zookeeper standalone server started.");
	}

	public void shutdown() {
		LOG.info("Shutting down zookeeper standalone server");
		if (factory != null) {
			factory.shutdown();
		}
		try {
			FileUtils.deleteDirectory(baseDir);
		} catch (IOException e) {
			LOG.error("Could not delete directory : {}", baseDir);
		}
		LOG.info("Zookeeper Standalone server shut down");
	}
}
