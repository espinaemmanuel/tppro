package ar.uba.fi.tppro.client;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public abstract class SearchDocsBase {
	
	protected final static Logger logger = LoggerFactory.getLogger(SearchDocsBase.class);
	
	public enum Destination{
		CORE,
		BROKER
	}

	/**
	 * @param args
	 * @throws NonExistentPartitionException 
	 * @throws TTransportException 
	 * @throws JsonSyntaxException 
	 * @throws JsonIOException 
	 * @throws TException 
	 * @throws PartitionAlreadyExistsException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws TTransportException, NonExistentPartitionException, IOException, TException  {
		
		Destination destination = Destination.valueOf(System.getProperty("destination", "CORE"));
		
		SearchDocsBase postDocs = null;
		
		switch(destination){
		case CORE:
			postDocs = new SearchDocsCore();
			break;
		case BROKER:
			postDocs = new SearchDocsBroker();
			break;
		}
		
		postDocs.run(args);
	}


	public abstract void run(String[] args) throws TTransportException, IOException, NonExistentPartitionException, TException;
	

}
