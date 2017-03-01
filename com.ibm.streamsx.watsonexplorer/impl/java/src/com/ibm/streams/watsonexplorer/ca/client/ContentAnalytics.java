package com.ibm.streams.watsonexplorer.ca.client;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.ibm.streams.watsonexplorer.AbstractRestCall;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.SearchResult;
import com.ibm.streams.watsonexplorer.WEXConnection;

public class ContentAnalytics extends AbstractRestCall {

	private static final String REST_URL_PATH = "/api/v10";
	
	private WEXConnection connection;
	private RestParameters staticParams;
	
	public ContentAnalytics(WEXConnection connection) {
		this(connection, new RestParameters());
	}
	
	public ContentAnalytics(WEXConnection connection, RestParameters params) {
		super(connection, params);
		this.connection = connection;
		this.staticParams = params;
	}
	
	/**
	 * Calls the /search REST API call
	 * @param parameters Additional parameters to pass to the search API.
	 * @return A SearchResult object
	 * @throws Exception
	 */
	public SearchResult search(RestParameters parameters) throws Exception {
		Response resp = execute("/search", parameters);
		
		return new CASearchResult(this, resp, parameters, true);
	}
	
	/**
	 * Calls the /collections REST API call
	 * @param parameters Additional parameters to pass to the collections API.
	 * @return A string containing the JSON or XML response.
	 * @throws Exception
	 */
	public String collections(RestParameters parameters) throws Exception {
		Response resp = execute("/collections", parameters);
		HttpResponse httpResp = resp.returnResponse();
		StatusLine status = httpResp.getStatusLine();
		if(status.getStatusCode() == 200) {
			return EntityUtils.toString(httpResp.getEntity());
		} else {
			throw new Exception(status.getStatusCode() + ": " + status.getReasonPhrase());
		}
	}
	
	/**
	 * Calls the /search/facet REST API call
	 * @param parameters Additional parameters to pass to the search API
	 * @return A SearchResult object
	 * @throws Exception
	 */
	public SearchResult searchFacet(RestParameters parameters) throws Exception {
		Response resp = execute("/search/facet", parameters);
		
		return new CASearchResult(this, resp, parameters, false);
	}
	
	/**
	 * Call the /analysis/text REST API call
	 * @param parameters Additional parameters to pass to the analysis API
	 * @return A SearchResult object
	 * @throws Exception
	 */
	public SearchResult analyzeText(RestParameters parameters) throws Exception {
		Response resp = execute("/analysis/text", parameters);
		
		return new CASearchResult(this, resp, parameters, false);
	}
	
	public RestParameters getStaticParameters() {
		return this.staticParams;
	}
	
	protected Response execute(String apiPath, RestParameters transientParameters) throws Exception {
		String url = connection.getConnectionUrl() + REST_URL_PATH + apiPath;
		return super.execute(url, transientParameters);
	}

	public SearchResult searchPreview(RestParameters parameters) throws Exception {
		Response resp = execute("/search/preview", parameters);
		return new CASearchResult(this, resp, parameters, false);
	}
}
