package com.ibm.streamsx.watsonexplorer.ca.client;

import java.io.ByteArrayInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.ibm.streamsx.watsonexplorer.HTTPRestException;
import com.ibm.streamsx.watsonexplorer.RestParameters;
import com.ibm.streamsx.watsonexplorer.SearchResult;
import com.ibm.streamsx.watsonexplorer.WEXConstants;

public class CASearchResult implements SearchResult {

	private ContentAnalytics ca;
	private RestParameters allParams;
	private RestParameters transientParams;
	private String content;
	private boolean hasMore;
	private Integer nextPage;
	private static DocumentBuilderFactory factory;
	private static DocumentBuilder builder;
	private static Gson gson;
	private boolean enablePaging;
	
	public CASearchResult(ContentAnalytics ca, Response resp, RestParameters params, boolean enablePaging) throws Exception {
		this.enablePaging = enablePaging;
		this.ca = ca;
		this.allParams = new RestParameters(params);
		this.allParams.putAll(ca.getStaticParameters());
		this.transientParams = new RestParameters(params);
		
		if(factory == null)
			factory = DocumentBuilderFactory.newInstance();
		if(builder == null)
			builder = factory.newDocumentBuilder();
		if(gson == null)
			gson = new Gson();
		
		HttpResponse httpResp = resp.returnResponse();
		StatusLine statusLine = httpResp.getStatusLine();
		if(statusLine.getStatusCode() == 200) {
			content = EntityUtils.toString(httpResp.getEntity());
			
			if(enablePaging) {
				if(this.allParams.containsKey("output")) {
					if(this.allParams.get("output").equals(WEXConstants.APPLICATION_JSON_OUTPUT)) {
						checkHasMoreJson();
					} else if(this.allParams.get("output").equals(WEXConstants.APPLICATION_XML_OUTPUT)) {
						checkHasMoreXML(false);
					} else if(this.allParams.get("output").equals(WEXConstants.APPLICATION_ATOM_XML_OUTPUT)) {
						checkHasMoreXML(true);
					}
				} else {
					checkHasMoreXML(true); // application/atom+xml is default output
				}	
			}
		} else {
			throw new HTTPRestException(statusLine);
		}
	}
	
	@Override
	public String getContent() {
		return content;
	}

	@Override
	public boolean hasMore() {
		return enablePaging && hasMore;
	}

	@Override
	public SearchResult next() throws Exception {
		if(hasMore()) {
			this.transientParams.put("page", nextPage.toString());
			return ca.search(this.transientParams);
		}

		return null;
	}

	private void checkHasMoreJson() {
		JSONSearchResult jsonResult = gson.fromJson(content, JSONSearchResult.class);
		if(jsonResult != null) {
			updateNextPage(jsonResult.apiResponse.numOfAvailableResults, jsonResult.apiResponse.itemsPerPage);	
		}
	}
	
	private void checkHasMoreXML(boolean isAtomXML) throws Exception {
		int numResults = 0;
		int itemsPerPage = 0;
		
		Document doc = builder.parse(new ByteArrayInputStream(content.getBytes()));
		
		// find # of available results
		NodeList nNumResults = doc.getElementsByTagName("es:numberOfAvailableResults");
		if(nNumResults.getLength() == 1) {
			numResults = Integer.valueOf(nNumResults.item(0).getTextContent());
		}

		// find # of items per page
		NodeList nNumItemsPerPage;
		if(isAtomXML) {
			nNumItemsPerPage = doc.getElementsByTagName("opensearch:itemsPerPage");			
		} else {
			nNumItemsPerPage = doc.getElementsByTagName("es:itemsPerPage");
		}

		if(nNumItemsPerPage.getLength() == 1) {
			itemsPerPage = Integer.valueOf(nNumItemsPerPage.item(0).getTextContent());
		} 
		
		updateNextPage(numResults, itemsPerPage);
	}
	
	private void updateNextPage(int numResults, int itemsPerPage) {
		boolean hasStart = false;
		boolean hasPage = false;
		
		hasStart = this.allParams.containsKey("start");
		hasPage = this.allParams.containsKey("page");
		
		if(hasPage) {
			int currentPage = Integer.valueOf((String)this.allParams.get("page"));
			nextPage = currentPage + 1;
		} else if(!hasPage && hasStart) {
			int start = Integer.valueOf((String)this.allParams.get("start"));
			nextPage = Math.floorDiv(start, itemsPerPage) + 1;
		} else {
			nextPage = 2; // current page = 1 if no page parameter is present
		}

		if(numResults == 0 || nextPage * itemsPerPage > numResults) {
			hasMore = false;
		} else { 
			hasMore = true;
		}		
	}
	
	private class JSONSearchResult { 
		class APIResponse {
			@SerializedName("es_numberOfAvailableResults")
			int numOfAvailableResults;
			
			@SerializedName("es_itemsPerPage")
			int itemsPerPage;
		}
		
		@SerializedName("es_apiResponse")
		APIResponse apiResponse;
	}
}
