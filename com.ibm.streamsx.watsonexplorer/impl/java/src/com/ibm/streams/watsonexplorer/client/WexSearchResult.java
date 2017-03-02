package com.ibm.streams.watsonexplorer.client;

import java.io.StringReader;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.ibm.streams.watsonexplorer.HTTPRestException;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.SearchResult;

public class WexSearchResult implements SearchResult {

	private Logger logger = Logger.getLogger(WexSearchResult.class);
	
	private WatsonExplorer wex;
	private RestParameters allParameters;
	private RestParameters transientParameters;
	private boolean isBrowse;
	private String content;
	private boolean hasMore;
	private String nextStart;
	
	public WexSearchResult(WatsonExplorer wex, Response resp, RestParameters parameters, boolean isBrowse) throws Exception {
		this.wex = wex;
		this.allParameters = new RestParameters(parameters);
		this.allParameters.putAll(wex.getStaticParameters());
		this.transientParameters = new RestParameters(parameters);
		this.isBrowse = isBrowse;
		
		HttpResponse httpResponse = resp.returnResponse();
		StatusLine status = httpResponse.getStatusLine();
		logger.trace("status=" + status);
		
		if(status.getStatusCode() == 200) {
			this.content = EntityUtils.toString(httpResponse.getEntity());
		} else {
			throw new HTTPRestException(status);
		}
		
		if(isBrowse) {
			try {
				String content = getContent();
					
				XMLInputFactory xmlFactory = XMLInputFactory.newFactory();
				XMLEventReader eventReader = xmlFactory.createXMLEventReader(new StringReader(content));
				while(eventReader.hasNext()) {
					XMLEvent event = eventReader.nextEvent();
					switch(event.getEventType()) {
					case XMLStreamConstants.START_ELEMENT:
						StartElement startElement = event.asStartElement();
						String qName = startElement.getName().getLocalPart();
						if(qName.equalsIgnoreCase("link")) {
							Attribute linkTypeAttr = startElement.getAttributeByName(new QName("type"));
							if(linkTypeAttr != null && linkTypeAttr.getValue().equals("next")) {
								this.hasMore = true;
								logger.trace("set hasMore=true");
								
								Attribute startTypeAttr = startElement.getAttributeByName(new QName("start"));
								if(startTypeAttr != null) {
									this.nextStart = startTypeAttr.getValue();
									logger.trace("set nextStart=" + this.nextStart);
								}
								
								break;
							}
						}
					}
				}					

			} catch (Exception e) {
				logger.error(e.getLocalizedMessage(), e);
			}			
		}		
	}
	
	@Override
	public String getContent() {
		return content;
	}

	@Override
	public boolean hasMore() {
		return isBrowse && hasMore;
	}

	@Override
	public SearchResult next() throws Exception {
		if (hasMore()) {
			this.transientParameters.put("browse-start", nextStart);
			return wex.querySearch(this.transientParameters);
		}
		return null;
	}
}
