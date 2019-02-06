package common;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class ConfigurationHandler {
	private static ConfigurationHandler instance;

	public static synchronized ConfigurationHandler getInstance() {
		if (instance == null) {
			instance = new ConfigurationHandler();
		}
		return instance;
	}	

	public String[] loadConfiguration(String name) {
		String[] args = new String[0];
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(name));
			ArrayList<String> props = new ArrayList<>();
			props.addAll(convertProperties(properties));
			args = props.toArray(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return args;
	}

	protected ArrayList<String> convertProperties(Properties properties) {
		if (properties.containsKey("use")) {
			Properties included = new Properties();
			String filename = properties.getProperty("use");
			try {
				included.load(new FileInputStream(filename));
				included.putAll(properties);
				properties = included;
			} catch (Exception e) {
				System.out.println("ERROR: Failed to load configuration from "+filename);
			}
		}
		ArrayList<String> props = new ArrayList<>();
		props.add("-H");
		props.add(properties.getProperty("hostname"));
		props.add("-p");
		props.add(properties.getProperty("port"));
		props.add("-DB");
		props.add(properties.getProperty("db"));
		if (properties.containsKey("user")) {
			props.add("-U");
			props.add(properties.getProperty("user"));
		}
		if (properties.containsKey("password")) {
			props.add("-P");
			props.add(properties.getProperty("password"));
		}
		if (properties.containsKey("authentication")) {
			props.add("-a");
			props.add(properties.getProperty("authentication"));
		}
		if (properties.containsKey("log")) {
			props.add("-d");
			props.add(properties.getProperty("log"));
		}
		if (properties.containsKey("progress")) {
			props.add("-RP");
		}
		if (properties.containsKey("separate_database")) {
			props.add("-sd");
		}

		return props;
	}

	//adapter from RefShark
	public void setLogLevel(String level) {
	    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

	    switch (level) {
	      case "INFO":
	        root.setLevel(Level.INFO);
	        break;
	      case "DEBUG":
	        root.setLevel(Level.DEBUG);
	        break;
	      case "WARNING":
	        root.setLevel(Level.WARN);
	        break;
	      case "ERROR":
	        root.setLevel(Level.ERROR);
	        break;
	      default:
	        root.setLevel(Level.DEBUG);
	        break;
	    }
  	}

}
