package tests;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.index.RemoteBrokerNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import tests.ReutersSaxParser.ReutersDoc;
import tests.ReutersSaxParser.ReutersDocHandler;

public class IndexReuters {
	
	public static AtomicInteger counter = new AtomicInteger(0);


	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, IndexNodeDescriptorException {
		
		Properties properties = new Properties();
		properties.load(new FileReader("config.properties"));
		
		String brokerHost = properties.getProperty("brokerHost");
		int port = Integer.parseInt(properties.getProperty("brokerPort"));
		int bufferSize = Integer.parseInt(properties.getProperty("bufferSize"));
		int group = Integer.parseInt(properties.getProperty("group"));
		File documentsDir = new File(properties.getProperty("documentsDir"));

		RemoteBrokerNodeDescriptor remoteNode = new RemoteBrokerNodeDescriptor(
				brokerHost, port);
		final IndexBroker.Iface broker = remoteNode.getClient();
		
		IndexingReutersDocHandler docHandler = new IndexingReutersDocHandler(broker, bufferSize, group, counter);
		ReutersSaxParser parser = new ReutersSaxParser(docHandler);

		for(File xmlFile : documentsDir.listFiles()){
			if(!xmlFile.getName().contains(".xml")) continue;
			
			parser.parse(xmlFile);
			System.out.println("Processed documents: " + counter);
		}
		
		System.out.println("Total time: " + docHandler.getIndexTime());		
	}

}
