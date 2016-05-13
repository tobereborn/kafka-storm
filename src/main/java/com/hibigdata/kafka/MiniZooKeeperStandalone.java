package com.hibigdata.kafka;

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
	private static final int DEFAULT_TICK_TIME = 500;
	private static final int DEFAULT_PORT = 2181;
	private static final int DEFAULT_MAX_CLENT_CNXNS = 1000;
	private static final String DEFAULT_HOST_NAME = "localhost";
	private static final String DEFAULT_SNAPSHOT_DIR = "snapshots";
	private static final String DEFAULT_LOG_DIR = "logs";
	private final int port;
	private final int tickTime;
	private final File baseDir;
	private final File snapshotDir;
	private final File logDir;
	private ServerCnxnFactory factory;

	public MiniZooKeeperStandalone(File baseDir) {
		this(baseDir, DEFAULT_PORT, DEFAULT_TICK_TIME);
	}

	public MiniZooKeeperStandalone(File baseDir, int port, int tickTime) {
		this.baseDir = baseDir;
		this.port = port;
		this.tickTime = tickTime;
		this.logDir = new File(baseDir, DEFAULT_LOG_DIR);
		this.snapshotDir = new File(baseDir, DEFAULT_SNAPSHOT_DIR);
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
			this.factory.configure(new InetSocketAddress(DEFAULT_HOST_NAME, port), DEFAULT_MAX_CLENT_CNXNS);
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
