package com.ibm.streams.watsonexplorer;

import java.util.Arrays;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

public abstract class AbstractRestCall {

	protected Executor executor;
	protected RestParameters params;
	private Logger logger = Logger.getLogger(AbstractRestCall.class);
	protected Form form;
	
	public AbstractRestCall(WEXConnection connection, RestParameters parameters) {
		this.params = parameters;
		
		executor = Executor.newInstance();
		if(connection.getCreds() != null)
			executor = executor.auth(connection.getCreds());

		form = Form.form();
		
		if(parameters != null)
			parameters.forEach((key, value) -> {
				try {
					String paramValue = getParamValue(key, value);
					form.add(key, paramValue.trim());	
				} catch(Exception e) {
					logger.warn("Unsupported type for parameter '" + key + "'. Skipping parameter.");
					return;
				}
			});
	}
	
	protected Response execute(String url, RestParameters transientParameters) throws Exception {
		List<NameValuePair> params = form.build();
		if(transientParameters != null) {
			
		}
			transientParameters.forEach((key, value) -> {
				try {
					String paramValue = getParamValue(key, value);
					params.add(new BasicNameValuePair(key, paramValue));	
				} catch(Exception e) {
					logger.warn("Unsupported type for parameter '" + key + "'. Skipping parameter.");
					return;
				}
			});

		logger.trace("rest_url=" + url);
		logger.trace("parameters=" + Arrays.toString(params.toArray()));
		Request req = Request.Post(url).bodyForm(params);
		Response resp = executor.execute(req);

		return resp;
	}
	
	private String getParamValue(String key, Object value) throws Exception {
		String paramValue = "";
		if(value instanceof String) {
			paramValue = (String)value;
		} else if(value instanceof Integer) {
			paramValue = Integer.toString((Integer)value);
		} else if(value instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<String> list = (List<String>)value;
			for(String s : list) {
				paramValue += s + " ";
			}
		} else {
			throw new Exception("Unsupported type");
		}
		
		return paramValue;
	}
}
