package com.ibm.streams.watsonexplorer.ca.client;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.ibm.streams.watsonexplorer.AbstractRestCall;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.SearchResult;
import com.ibm.streams.watsonexplorer.WEXConnection;
import com.ibm.streams.watsonexplorer.ca.client.admin.AdminApiUtil;
import com.ibm.streams.watsonexplorer.ca.client.admin.AdminError;

public class ContentAnalytics extends AbstractRestCall {

	private static final String REST_V10_URL_PATH = "/api/v10";
	private static final String REST_V20_URL_PATH = "/api/v20";

	private static final String ADD_TEXT_PATH = "/admin/collections/indexer/document/add/text";
	
	private static final Logger logger = Logger.getLogger("ContentAnalytics.class");
	private static final int MAX_RETRIES = 10;
	
	private WEXConnection connection;
	private RestParameters staticParams;
	private String securityToken;
	
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
	public String collections(RestParameters parameters, boolean useAdminAPI) throws Exception {
		parameters.put("output", "application/json");

		Response resp;
		if(useAdminAPI) {
			String securityToken = getSecurityToken();
			parameters.put("securityToken", securityToken);
			resp = executeV20("/admin/collections/list", parameters);
		} else {
			resp = execute("/collections", parameters);	
		}
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
	
	/**
	 * Call the /search/preview REST API call
	 * @param parameters Additional parameters to pass to the analysis API
	 * @return A SearchResult object
	 * @throws Exception
	 */
	public SearchResult searchPreview(RestParameters parameters) throws Exception {
		Response resp = execute("/search/preview", parameters);
		return new CASearchResult(this, resp, parameters, false);
	}
	
	public String adminLogin() throws Exception {
		RestParameters params = new RestParameters();
		params.put("username", connection.getCreds().getUserPrincipal().getName());
		params.put("password", connection.getCreds().getPassword());
		params.put("output", "application/json");
		
		logger.trace("Starting admin login...");
		Response resp = executeV20("/admin/login", params);
		this.securityToken = AdminApiUtil.parseSecurityToken(resp);
		if(securityToken != null) {
			logger.trace("Admin login successful! secToken=" + securityToken);
			return securityToken;
		} else {
			// we shouldn't get here, but if we do then more error 
			// handling code needs to be added to SecurityTokenUtil 
			throw new Exception("Unknown error retrieving security token for user: " + connection.getCreds().getUserPrincipal().getName());
		}
	}
	
	public void adminLogout() throws Exception {
		if(securityToken == null)
			return;
		
		params.put("securityToken", securityToken);
		params.put("output", "application/json");
		
		Response resp = executeV20("/admin/logout", params);
		HttpResponse httpResp = resp.returnResponse();
		StatusLine statusLine = httpResp.getStatusLine();
		String content = EntityUtils.toString(httpResp.getEntity());
		if(statusLine.getStatusCode() != 200) {
			logger.trace("Received status code " + statusLine.getStatusCode() + " during logout. [content=" + content + "]");
		}
	}
	
	private String getSecurityToken() throws Exception {
		if(securityToken == null || securityToken.isEmpty()) {
			logger.trace("Security token not found! Initiating login...");
			adminLogin();
		}
		
		return securityToken;
	}
	
	private void expireSecurityToken() throws Exception {
		adminLogout();
		securityToken = null;
	}
	
	private boolean isAuthError(AdminError error) {
		return AdminError.AUTH_ERROR_CODE.equalsIgnoreCase(error.getCode());
	}
	
	private APIResponse adminAPICall(String apiUrl, RestParameters parameters) throws Exception {		
		APIResponse response = new APIResponse();
		int retriesRemaining = MAX_RETRIES;
		while(retriesRemaining > 0) {
			String secToken = getSecurityToken();
			parameters.put("securityToken", secToken);
			logger.trace("Retrieved security token: " + secToken);
			
			Response resp = executeV20(apiUrl, parameters);
			HttpResponse httpResp = resp.returnResponse();
			StatusLine statusLine = httpResp.getStatusLine();
			String content = EntityUtils.toString(httpResp.getEntity());

			response.setContent(content);
			response.setStatus(statusLine);
			
			int statusCode = statusLine.getStatusCode();			
			if(statusCode == 500) {
				AdminError adminError = AdminApiUtil.parseErrorMessage(content);
				if(isAuthError(adminError)) {
					// expire the old security token
					// and acquire a new one
					expireSecurityToken();
					adminLogin();
					
					retriesRemaining--;
					continue;
				}
			} else {
				break;
			}
		}
		
		return response;
	}
	
	/**
	 * Call the /admin/collections/indexer/document/add/text REST API call
	 * @param documentId The ID of the document
	 * @param text The document content
	 * @param parameters Additional parameters to pass to the analysis API 
	 * @return true if the document was added to the collection, false otherwise
	 * @throws Exception an Exception will be thrown if there was a failure authenticating the user
	 */
	public boolean adminAddText(RestParameters parameters) throws Exception {
		logger.trace("Starting adminAddText...");
		logger.trace("adminAddText parameters=" + parameters);
		
		APIResponse response = adminAPICall(ADD_TEXT_PATH, parameters);
		if(response.getStatus().getStatusCode() == 200) {
			logger.trace("adminAddText SUCCESS!");
			return true; // add text was successful
		} else {
			logger.trace("adminAddText FAILED!");
			
			AdminError error = AdminApiUtil.parseErrorMessage(response.getContent());
			String errorMessage = "";
			if(error.getCode().equals("500") && isAuthError(error)) {
				errorMessage = error.getMessage();
				throw new AuthenticationException(errorMessage);
			} else {
				errorMessage = "Unexpected response (status code) from server: statusCode=" + response.getStatus().getStatusCode() + ", responseText=" + response.getContent();
				throw new Exception(errorMessage);
			}
		}
	}
	
	public String adminGetFields(RestParameters parameters) throws Exception {
		logger.trace("Starting adminGetFields...");
		logger.trace("adminGetFields parameters=" + parameters);
		
		APIResponse response = adminAPICall("/admin/collections/indexer/fields/list", parameters);
		if(response.getStatus().getStatusCode() == 200) {
			logger.trace("adminGetFields SUCCESS!");
			return response.getContent(); // add text was successful
		} else {
			logger.trace("adminAddText FAILED!");
			String errorMessage;
			if(response.getStatus().getStatusCode() == 500) {
				errorMessage = AdminApiUtil.parseErrorMessage(response.getContent()).getMessage();
			} else {
				errorMessage = "Unexpected response (status code) from server: statusCode=" + response.getStatus().getStatusCode() + ", responseText=" + response.getContent();
			}
			
			throw new AuthenticationException(errorMessage);
		}
	}
	
	public RestParameters getStaticParameters() {
		return this.staticParams;
	}

	protected Response execute(String baseRestURL, String apiPath, RestParameters transientParameters) throws Exception {
		String url = connection.getConnectionUrl() + baseRestURL + apiPath;
		return super.execute(url, transientParameters);		
	}
	
	protected Response execute(String apiPath, RestParameters transientParameters) throws Exception {
		return execute(REST_V10_URL_PATH, apiPath, transientParameters);
	}

	protected Response executeV20(String apiPath, RestParameters transientParameters) throws Exception {
		return execute(REST_V20_URL_PATH, apiPath, transientParameters);
	}
	
	private static class APIResponse {
		private String content;
		private StatusLine status;
		
		public APIResponse() {
		}

		public void setContent(String content) {
			this.content = content;
		}
		
		public void setStatus(StatusLine status) {
			this.status = status;
		}
		
		public String getContent() {
			return content;
		}
		
		public StatusLine getStatus() {
			return status;
		}
	}
}
