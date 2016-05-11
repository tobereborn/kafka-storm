package com.hibigdata.kafka.util;

import java.io.File;

public class FileUtils {
	public static File mkdir(File parent, String child) {
		File file = new File(parent, child);
		if (!file.mkdirs()) {
			throw new RuntimeException("Could not create dir " + file.getAbsolutePath());
		}
		file.deleteOnExit();
		return file;
	}

	public static void remove(File file) {
		if (file.isDirectory()) {
			for (File child : file.listFiles()) {
				remove(child);
			}
		} else {
			file.delete();
		}
	}
}
