package com.ibm.streams.watsonexplorer.ca.client;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class SecurityTokenUtil {

	private static Gson gson = new Gson();
	
	public static String parseSecurityToken(Response resp) throws Exception {
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
				throw new Exception("Response does not contain a security token: " + content);
			}
		} else if(statusCode == 500) {
			JsonObject jsonObj = gson.fromJson(content, JsonObject.class);
			if(jsonObj != null && jsonObj.has("error")) {
				JsonObject errorObj = jsonObj.getAsJsonObject("error");
				if(errorObj.has("message")) {
					String msg = errorObj.get("message").getAsString();
					throw new Exception("Error retrieving security token: " + msg);
				}
			}
		} else {
			throw new Exception("Unexpected response (status code) from server: statusCode=" + statusCode + ", responseText=" + content);
		}
		
		return null;
	}
}
