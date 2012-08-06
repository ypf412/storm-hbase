package ypf412.storm.util;

/**
 * This is a configure loader class for properties file.
 * 
 * @author jiuling.ypf
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropConfig {

	private Properties props = null;

	public PropConfig() {
		props = new Properties();
	}

	public PropConfig(String path) throws IOException {
		props = new Properties();
		loadResource(path);
	}

	public void loadResource(String path) throws IOException {
		if (!path.startsWith("/")) { // see relative path as system resource
			InputStream in = ClassLoader.getSystemResourceAsStream(path);
			props.load(in);
			in.close();
		} else {
			FileReader fr = new FileReader(path);
			props.load(fr);
			fr.close();
		}
	}

	public String getProperty(String key) {
		return props.getProperty(key);
	}
}
