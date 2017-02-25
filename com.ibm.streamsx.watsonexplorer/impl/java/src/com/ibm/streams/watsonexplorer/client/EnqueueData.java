package com.ibm.streams.watsonexplorer.client;

import java.io.ByteArrayInputStream;
import java.net.URI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.ibm.streams.watsonexplorer.HTTPRestException;

public class EnqueueData {

	String formattedData;

	public static EnqueueData instance;
	
	private static DocumentBuilderFactory factory;
	private static DocumentBuilder builder;

	private Logger logger = Logger.getLogger(EnqueueData.class);
	
	private EnqueueData() throws Exception {
		factory = DocumentBuilderFactory.newInstance();
		builder = factory.newDocumentBuilder();
	}

	public static EnqueueData getInstance() throws Exception {
		if(instance == null)
			instance = new EnqueueData();
		
		return instance;
	}

	public String generateOutput(EnqueueDataOptions options) {
		return String.format(
				"<crawl-url status=\"complete\" url=\"%s\" synchronization=\"%s\" enqueue-type=\"%s\">\n"
						+ "  <crawl-data>\n" + "    <text>%s</text>\n" + "  </crawl-data>\n" + "</crawl-url>",
				options.getUrl(), options.getSynchronization(), options.getEnqueueType(), options.getData());
	}

	public boolean isSuccess(Response response) throws Exception {
		HttpResponse httpResp = response.returnResponse();
		StatusLine status = httpResp.getStatusLine();
		if(status.getStatusCode() != 200) {
			throw new HTTPRestException(status);
		}
		
		String content = EntityUtils.toString(httpResp.getEntity());
		
		Document doc = builder.parse(new ByteArrayInputStream(content.getBytes()));
		NodeList nNodes = doc.getElementsByTagName("crawl-url");
		if(nNodes.getLength() == 1) {
			Element elem = (Element)nNodes.item(0);
			String attribute = elem.getAttribute("state");
			if(attribute.equals("success")) {
				return true;
			} else {
				logger.error("Error enqueuing data: " + content);
				return false;
			}
		}
		
		return false;
	}
	
	public static class EnqueueDataOptions {

		private String data;
		private String url;
		private String enqueueType = "forced";
		private String synchronization = "none";

		public String getData() {
			return data;
		}

		public void setData(String data) {
			this.data = data;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			URI uri = URI.create(url);
			this.url = uri.getScheme() == null ? "ref://" + url : url;
		}

		public String getEnqueueType() {
			return enqueueType;
		}

		public void setEnqueueType(String enqueueType) {
			this.enqueueType = enqueueType;
		}

		public String getSynchronization() {
			return synchronization;
		}

		public void setSynchronization(String synchronization) {
			this.synchronization = synchronization;
		}

	}
}
