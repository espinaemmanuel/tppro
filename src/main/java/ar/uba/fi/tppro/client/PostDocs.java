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

public class PostDocs {

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 * @throws JsonSyntaxException 
	 * @throws JsonIOException 
	 * @throws TException 
	 * @throws PartitionAlreadyExistsException 
	 */
	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException, PartitionAlreadyExistsException, TException {
		String host = System.getProperty("host", "localhost");
		String port = System.getProperty("port", "9090");
		int partition = Integer.parseInt(System.getProperty("partition", "0"));

		TTransport transport = new TSocket(host, Integer.parseInt(port));
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();
		
		if(args.length == 0){
			System.out.println("Missing files");
			return;
		}
		
		if(!client.containsPartition(partition)){
			client.createPartition(partition);
		}
		
		for(String jsonFile : args){
			indexFile(jsonFile, client, partition);
		}

	}

	private static void indexFile(String jsonFile, IndexNode.Client client, int partition) throws JsonIOException, JsonSyntaxException, FileNotFoundException, NonExistentPartitionException, TException {
		

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
		
		client.index(partition, documents);
			
	}

	private static Document jsonToDocument(JsonObject jsonObject) {
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
