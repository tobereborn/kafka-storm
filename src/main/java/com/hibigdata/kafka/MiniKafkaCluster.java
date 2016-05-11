package com.hibigdata.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.hibigdata.kafka.util.FileUtils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class MiniKafkaCluster {
	private static final File LOG_DIR = new File("kafka_logs");
	private String zk;
	private List<Integer> ports = new ArrayList<Integer>();
	private List<KafkaServerStartable> brokers = new ArrayList<KafkaServerStartable>();

	public MiniKafkaCluster(String zk, List<Integer> ports) {
		this.zk = zk;
		this.ports.addAll(ports);
	}

	public void startup() {
		for (Integer port : ports) {
			File brokerLogDir = FileUtils.mkdir(LOG_DIR, "borker" + port);
			Properties props = new Properties();
			props.put("zookeeper.connect", zk);
			props.put("broker.id", port.toString());
			props.put("host.name", "localhost");
			props.put("port", port.toString());
			props.put("log.dir", brokerLogDir.getAbsolutePath());
			KafkaServerStartable broker = new KafkaServerStartable(new KafkaConfig(props));
			System.out.println("Start broker @" + "localhost:" + port);
			broker.startup();
			System.out.println("Start broker @" + "localhost:" + port);
			brokers.add(broker);
		}
	}

	public void shutdown() {
		for (KafkaServerStartable broker : brokers) {
			broker.shutdown();
			System.out.println("Shutdown broker " + broker);
		}
		FileUtils.remove(LOG_DIR);
	}

	public static void main(String[] args) {
		MiniKafkaCluster cluster = null;
		MiniZookeeper zookeeper = null;
		try {
			zookeeper = new MiniZookeeper();
			cluster = new MiniKafkaCluster("localhost:2181", Arrays.asList(new Integer[] { 9001, 9002, 9003 }));
			zookeeper.startup();
			cluster.startup();
			TimeUnit.SECONDS.sleep(30);
		} catch (InterruptedException e) {

		} finally {
			if (cluster != null) {
				cluster.shutdown();
			}
			if (zookeeper != null) {
				zookeeper.shutdown();
			}
		}
	}
}
