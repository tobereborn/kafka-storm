package com.github.tbr.kafka;

import static com.github.tbr.kafka.util.ClusterConstants.DEFAULT_HOST_NAME;
import static com.github.tbr.kafka.util.ClusterConstants.KAFKA_BROKER_ID;
import static com.github.tbr.kafka.util.ClusterConstants.KAFKA_HOST_NAME;
import static com.github.tbr.kafka.util.ClusterConstants.KAFKA_LOG_DIR;
import static com.github.tbr.kafka.util.ClusterConstants.KAFKA_PORT;
import static com.github.tbr.kafka.util.ClusterConstants.KAFKA_ZK_CONNECT;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class MiniKafkaCluster {
	private static final Logger LOG = LoggerFactory.getLogger(MiniKafkaCluster.class);

	private final String zkConnect;
	private final List<Integer> clientPorts;
	private final List<KafkaServerStartable> brokers;
	private final File baseDir;

	public MiniKafkaCluster(String zkConnect, File baseDir, List<Integer> clientPorts) {
		this.zkConnect = zkConnect;
		this.clientPorts = new ArrayList<Integer>(clientPorts);
		this.brokers = new ArrayList<KafkaServerStartable>();
		this.baseDir = baseDir;
	}

	public void startup() {
		LOG.info("Starting brokers...");
		for (Integer port : clientPorts) {
			KafkaServerStartable broker = new KafkaServerStartable(new KafkaConfig(brokerProps(port)));
			broker.startup();
			brokers.add(broker);
		}
		LOG.info("All brokers started");
	}

	private Properties brokerProps(Integer port) {
		Properties props = new Properties();
		String brokerName = "broker_" + port;
		File logDir = new File(baseDir, brokerName);
		try {
			FileUtils.forceMkdir(logDir);
		} catch (IOException e) {
			LOG.error("Could not create log dir {}", logDir.getAbsolutePath());
			throw new RuntimeException(e);
		}

		props.put(KAFKA_ZK_CONNECT, zkConnect);
		props.put(KAFKA_BROKER_ID, port.toString());
		props.put(KAFKA_HOST_NAME, DEFAULT_HOST_NAME);
		props.put(KAFKA_PORT, port.toString());
		props.put(KAFKA_LOG_DIR, logDir.getAbsolutePath());
		return props;
	}

	public void shutdown() {
		LOG.info("Shuting down brokers...");
		for (KafkaServerStartable broker : brokers) {
			broker.shutdown();
		}
		try {
			FileUtils.deleteDirectory(baseDir);
		} catch (IOException e) {
			LOG.error("Could not delete base directory: {}", baseDir.getAbsolutePath());
			throw new RuntimeException(e);
		}
		LOG.info("All brokers shut down.");
	}

}
