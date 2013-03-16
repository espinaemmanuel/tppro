package ar.uba.fi.tppro.console.commands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class IndexJson implements Command {

	public String getName() {
		return "indexJson";
	}

	public void execute(String[] argv, Context context) throws Exception {
		if (context.currentNode == null) {
			System.out.println("Current node not selected");
		}

		String[] nodeParts = context.currentNode.split("_");

		RemoteIndexNodeDescriptor remoteNode = new RemoteIndexNodeDescriptor(
				nodeParts[0], Integer.parseInt(nodeParts[1]));
		IndexNode.Iface indexNode = remoteNode.getClient();
		
		int groupId = Integer.parseInt(argv[1]);
		int partitionId = Integer.parseInt(argv[2]);
		int messageId = Integer.parseInt(argv[3]);
		
		File jsonFile = new File(argv[4]);
		
		if(!jsonFile.exists()){
			throw new FileNotFoundException("file not found");
		}
		
		indexNode.prepareCommit(groupId, partitionId, messageId, createDocuments(jsonFile));
		indexNode.commit(groupId, partitionId);

	}
	
	protected List<Document> createDocuments(File jsonFile) throws JsonIOException, JsonSyntaxException, FileNotFoundException{
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
