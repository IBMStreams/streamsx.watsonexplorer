package com.ibm.streams.watsonexplorer.ca.client.admin;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AdminApiUtil {

	private static Gson gson = new Gson();

	public static AdminError parseErrorMessage(String jsonStr) throws Exception {
		JsonObject jsonObj = gson.fromJson(jsonStr, JsonObject.class);
		if(jsonObj.has("error")) {
			AdminError adminError = gson.fromJson(jsonObj.get("error"), AdminError.class);
			return adminError;
		} else {
			throw new Exception("String does not contain a JSON error message: " + jsonStr);
		}
	}
	
	public static String parseSecurityToken(Response resp) throws AuthenticationException, Exception {
		HttpResponse httpResp = resp.returnResponse();
		StatusLine statusLine = httpResp.getStatusLine();
		int statusCode = statusLine.getStatusCode();
		String content = EntityUtils.toString(httpResp.getEntity());
		if(statusCode == 200) {
			JsonObject jsonObj = gson.fromJson(content, JsonObject.class);
			JsonObject apiRespObj = jsonObj.getAsJsonObject("es_apiResponse");
			if(apiRespObj != null && apiRespObj.has("es_securityToken")) {
				return apiRespObj.get("es_securityToken").getAsString();
			} else {
				throw new AuthenticationException("Response does not contain a security token: " + content);
			}
		} else if(statusCode == 500) {
			String errorMessage = parseErrorMessage(content).getMessage();
			throw new AuthenticationException("Error retrieving security token: " + errorMessage);
		} else {
			throw new Exception("Unexpected response (status code) from server: statusCode=" + statusCode + ", responseText=" + content);
		}
	}	
}









