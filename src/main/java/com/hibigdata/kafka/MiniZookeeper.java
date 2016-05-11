package com.hibigdata.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public class MiniZookeeper {
	private static final int TICK_TIME = 500;
	private static final int PORT = 2181;
	private static final File SNAPSHOT_DIR = new File("zk/snapshots");
	private static final File LOG_DIR = new File("zk/logs");
	private ServerCnxnFactory factory;

	public MiniZookeeper() {
		SNAPSHOT_DIR.deleteOnExit();
		LOG_DIR.deleteOnExit();
	}

	public void startup() {
		try {
			ZooKeeperServer zkServer = new ZooKeeperServer(SNAPSHOT_DIR, LOG_DIR, TICK_TIME);
			this.factory = NIOServerCnxnFactory.createFactory();
			this.factory.configure(new InetSocketAddress("localhost", PORT), 16);
			this.factory.startup(zkServer);
		} catch (IOException e) {
			throw new RuntimeException("Unable to startup zk");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public void shutdown() {
		if (factory != null) {
			factory.shutdown();
		}
	}

	public void startupInternal() {
		Properties startupProperties = new Properties();

		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(startupProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);

		new Thread() {
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					// log.error("ZooKeeper Failed", e);
				}
			}
		}.start();
	}

	public static void main(String[] args) {
		MiniZookeeper zk = new MiniZookeeper();
		zk.startup();
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		zk.shutdown();
	}

}
