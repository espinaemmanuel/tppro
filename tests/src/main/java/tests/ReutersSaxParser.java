package tests;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class ReutersSaxParser {

	public class ReutersDoc {
		public String title;
		public String text;
	}

	public interface ReutersDocHandler {
		public void handle(ReutersDoc doc);
	}

	private SAXParser saxParser;
	DefaultHandler handler = new DefaultHandler() {

		StringBuffer title = new StringBuffer();
		StringBuffer text = new StringBuffer();

		boolean inTitle = false;
		boolean inBody = false;

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {

			if (qName.equalsIgnoreCase("REUTERS")) {
				title.setLength(0);
				text.setLength(0);
			}

			if (qName.equalsIgnoreCase("TITLE")) {
				inTitle = true;
			}

			if (qName.equalsIgnoreCase("BODY")) {
				inBody = true;
			}
		}

		public void endElement(String uri, String localName, String qName)
				throws SAXException {

			if (qName.equalsIgnoreCase("REUTERS")) {
				ReutersDoc newDoc = new ReutersDoc();
				newDoc.title = title.toString();
				newDoc.text = text.toString();
				
				reutersDocHandler.handle(newDoc);
			}

			if (qName.equalsIgnoreCase("TITLE")) {
				inTitle = false;
			}

			if (qName.equalsIgnoreCase("BODY")) {
				inBody = false;
			}

		}

		@Override
		public void characters(char ch[], int start, int length)
				throws SAXException {
			if(inTitle){
				this.title.append(ch, start, length);
			}
			
			if(inBody){
				this.text.append(ch, start, length);
			}
		}
	};
	
	private ReutersDocHandler reutersDocHandler;

	public ReutersSaxParser(ReutersDocHandler reutersDocHandler) throws ParserConfigurationException,
			SAXException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		saxParser = factory.newSAXParser();
		this.reutersDocHandler = reutersDocHandler;
	}

	public void parse(File f) throws SAXException, IOException {
		
		Reader isr = new FileReader(f);
		InputSource is = new InputSource();
		is.setCharacterStream(isr);		
		
		saxParser.parse(is, handler);
	}

}
