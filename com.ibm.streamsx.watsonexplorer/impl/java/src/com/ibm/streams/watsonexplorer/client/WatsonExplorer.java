package com.ibm.streams.watsonexplorer.client;

import org.apache.http.client.fluent.Response;

import com.ibm.streams.watsonexplorer.AbstractRestCall;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.SearchResult;
import com.ibm.streams.watsonexplorer.WEXConnection;
import com.ibm.streams.watsonexplorer.client.EnqueueData.EnqueueDataOptions;

public class WatsonExplorer extends AbstractRestCall {

	private static final String REST_URL_PATH = "/vivisimo/cgi-bin/velocity";
	private WEXConnection connection;
	private RestParameters staticParams;

	public WatsonExplorer(WEXConnection connection) {
		this(connection, new RestParameters());
	}

	public WatsonExplorer(WEXConnection connection, RestParameters parameters) {
		super(connection, parameters);
		this.connection = connection;
		this.staticParams = parameters;

		form.add("v.app", "api-rest");
		form.add("v.username", connection.getCreds().getUserPrincipal().getName());
		form.add("v.password", connection.getCreds().getPassword());
	}

	public SearchResult querySearch(RestParameters params) throws Exception {
		Response resp = execute("query-search", params);

		boolean isBrowse = (params.containsKey("browse") && params.get("browse").equals("true"))
				|| (this.staticParams.containsKey("browse") && this.staticParams.get("browse").equals("true"));

		return new WexSearchResult(this, resp, params, isBrowse);
	}

	public boolean enqueueData(EnqueueDataOptions options, RestParameters transientParameters)
			throws Exception {
		String crawlUrlData = EnqueueData.getInstance().generateOutput(options);
		RestParameters params = new RestParameters(transientParameters);
		params.put("crawl-urls", crawlUrlData);
		
		Response resp = execute("search-collection-enqueue", params);
		return EnqueueData.getInstance().isSuccess(resp);
	}

	public RestParameters getStaticParameters() {
		return this.staticParams;
	}

	protected Response execute(String function, RestParameters transientParameters) throws Exception {
		String url = connection.getConnectionUrl() + REST_URL_PATH;
		RestParameters params = new RestParameters(transientParameters);
		params.put("v.function", function);

		return super.execute(url, params);
	}
}
