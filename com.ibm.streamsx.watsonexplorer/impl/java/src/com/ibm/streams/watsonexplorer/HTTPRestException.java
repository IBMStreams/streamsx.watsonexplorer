package com.ibm.streams.watsonexplorer;

import org.apache.http.StatusLine;

public class HTTPRestException extends Exception {

	private static final long serialVersionUID = 1L;

	public HTTPRestException(StatusLine statusLine) {
		super(statusLine.getStatusCode() + ": " + statusLine.getReasonPhrase());
	}
	
}
