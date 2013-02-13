package ar.uba.fi.tppro.core.index;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;


public class TestUtil {
	
	public static IndexNode.Client getCoreClient(String host, int port) throws TTransportException{
		
		TTransport transport = new TSocket("localhost", port);
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();
		
		return client;
		
	}
	
	public static List<Document> createDocuments(String jsonFile) throws JsonIOException, JsonSyntaxException, FileNotFoundException{
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
