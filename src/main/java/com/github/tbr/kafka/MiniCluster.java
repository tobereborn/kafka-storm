package com.github.tbr.kafka;

import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_HOST_NAME;
import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_ZK_PORT;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to facilitate kafka-storm data process stream unit testing.
 * 
 */
public class MiniCluster {
	private static final Logger LOG = LoggerFactory.getLogger(MiniCluster.class);
	private final MiniZooKeeperStandalone zookeeper;
	private final MiniKafkaCluster kafka;

	public MiniCluster(File baseDir, List<Integer> kafkaPorts) {
		this.zookeeper = new MiniZooKeeperStandalone(baseDir);
		this.kafka = new MiniKafkaCluster(DEFAULT_HOST_NAME + ":" + DEFAULT_ZK_PORT, baseDir, kafkaPorts);
	}

	public void startup() {
		LOG.info("Starting local cluster...");
		zookeeper.startup();
		kafka.startup();
		LOG.info("Local cluster started.");
	}

	public void shutdown() {
		LOG.info("Shuting down local cluster...");
		kafka.shutdown();
		zookeeper.shutdown();
		LOG.info("Local cluster shut down");
	}

	public static void main(String[] args) {
		MiniCluster cluster = null;
		try {
			cluster = new MiniCluster(new File("target/cluster"), Arrays.asList(new Integer[] { 9001, 9002, 9003 }));
			cluster.startup();
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}
}
