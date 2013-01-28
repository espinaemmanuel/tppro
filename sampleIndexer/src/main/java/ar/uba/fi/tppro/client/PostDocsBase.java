package ar.uba.fi.tppro.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public abstract class PostDocsBase {
	
	protected final static Logger logger = LoggerFactory.getLogger(PostDocsBase.class);
	
	public enum Destination{
		CORE,
		BROKER
	}

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 * @throws JsonSyntaxException 
	 * @throws JsonIOException 
	 * @throws TException 
	 * @throws PartitionAlreadyExistsException 
	 */
	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException, PartitionAlreadyExistsException, TException {
		
		Destination destination = Destination.valueOf(System.getProperty("destination", "CORE"));
		
		PostDocsBase postDocs = null;
		
		switch(destination){
		case CORE:
			postDocs = new PostDocsCore();
			break;
		case BROKER:
			postDocs = new PostDocsBroker();
			break;
		}
		
		postDocs.run(args);


	}
	
	protected abstract void run(String[] args) throws TTransportException, PartitionAlreadyExistsException, TException, JsonIOException, JsonSyntaxException, FileNotFoundException;

	protected List<Document> createDocuments(String jsonFile) throws JsonIOException, JsonSyntaxException, FileNotFoundException{
		JsonParser parser = new JsonParser();
		JsonElement element = parser.parse(new BufferedReader(new FileReader(jsonFile)));
		List<Document> documents = Lists.newArrayList();
				
		if(element.isJsonObject()){
			Document doc = jsonToDocument(element.getAsJsonObject());
			documents.add(doc);
		} else if (element.isJsonArray()){
			for(JsonElement object : element.getAsJsonArray()){
				if(object.isJsonObject()){
					documents.add(jsonToDocument(object.getAsJsonObject()));
				}
			}
		}
		
		return documents;
	}
	
	protected static Document jsonToDocument(JsonObject jsonObject) {
		Document document = new Document();
		document.fields = Maps.newHashMap();
		
		for(Map.Entry<String, JsonElement> entry : jsonObject.entrySet()){
			String key = entry.getKey();
			
			String val = null;
			if(entry.getValue().isJsonPrimitive()){
				val = entry.getValue().getAsString();
			}
			
			if(entry.getValue().isJsonArray()){
				val = entry.getValue().toString();
			}
						
			if(val == null || val.isEmpty()) continue;
			
			document.fields.put(key, val);
		}
		
		return document;
	}

}
