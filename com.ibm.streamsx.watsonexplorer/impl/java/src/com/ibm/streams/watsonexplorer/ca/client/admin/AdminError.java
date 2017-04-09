package com.ibm.streams.watsonexplorer.ca.client.admin;

public class AdminError {
	
	public static final String AUTH_ERROR_CODE = "FFQED0261E";
	
	private String code;
	private String message;
	private String detail;
	
	public String getCode() {
		return code;
	}
	
	public String getDetail() {
		return detail;
	}
	
	public String getMessage() {
		return message;
	}
	
}
