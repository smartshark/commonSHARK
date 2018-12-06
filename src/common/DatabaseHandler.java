package common;

import java.util.ArrayList;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class DatabaseHandler {
	
	public static Datastore createDatastore(String hostname, int port, String db) {
	    Morphia morphia = new Morphia();
	    morphia.mapPackage("de.ugoe.cs.smartshark.model");
	    Datastore datastore = null;
	    try {
	        datastore = morphia.createDatastore(
	        		new MongoClient(
	        				hostname, 
	        				port), 
	        		db);
	    } catch (Exception e) {
		      System.err.println(e.getMessage());
		      e.printStackTrace(System.err);
		      System.exit(1);
	    }
	
	    return datastore;
	}
	
	public static Datastore createDatastore(Parameter parameter) {
	    Morphia morphia = new Morphia();
	    morphia.mapPackage("de.ugoe.cs.smartshark.model");
	    Datastore datastore = null;

	    try {
	      if (parameter.getDbUser().isEmpty()
	    		  || parameter.getDbPassword().isEmpty()) {
	        datastore = morphia.createDatastore(
	        		new MongoClient(
	        				parameter.getDbHostname(), 
	        				parameter.getDbPort()), 
	        		parameter.getDbName());
	      } else {
	        ServerAddress addr = new ServerAddress(
	        		parameter.getDbHostname(), 
	        		parameter.getDbPort());
	        List<MongoCredential> credentialsList = new ArrayList<>();
	        MongoCredential credential = MongoCredential.createCredential(
	            parameter.getDbUser(), 
	            parameter.getDbAuthentication(), 
	            parameter.getDbPassword().toCharArray());
	        credentialsList.add(credential);
	        MongoClient client = new MongoClient(addr, credentialsList);
	        datastore = morphia.createDatastore(
	        		client, 
	        		parameter.getDbName());
	      }
	    } catch (Exception e) {
	      System.err.println(e.getMessage());
	      e.printStackTrace(System.err);
	      System.exit(1);
	    }

	    return datastore;
	}

}
