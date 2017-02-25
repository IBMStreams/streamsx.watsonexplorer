package com.ibm.streams.watsonexplorer.ca.client;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.WEXConnection;

public class CollectionsUtil {

	private static Map<String /* collectionName */, String /* collectionId */> collectionNameCache = new HashMap<String, String>();
	
	public static String getCollectionId(WEXConnection connection, String collectionName) throws Exception {
		// return the ID in the cache if it exists
		if(collectionNameCache.containsKey(collectionName))
			return collectionNameCache.get(collectionName);
		
		ContentAnalytics ca = new ContentAnalytics(connection);
		String content = ca.collections(new RestParameters());
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new ByteArrayInputStream(content.getBytes()));
		
		NodeList entryList = doc.getElementsByTagName("atom:entry");
		for(int i = 0; i < entryList.getLength(); ++i) {
			Element elem = (Element)entryList.item(i);
			NodeList titleElements = elem.getElementsByTagName("atom:title");
			if(titleElements.getLength() == 1) {
				Element title = (Element)titleElements.item(0);
				if(title.getTextContent().equals(collectionName)) {
					NodeList idElements = elem.getElementsByTagName("atom:id");
					if(idElements.getLength() == 1) {
						Element id = (Element)idElements.item(0);
						String collectionId = id.getTextContent();
						collectionNameCache.put(collectionName, collectionId);
						
						return collectionId;
					}
				}
			}
		}
		
		return null;
	}
}
