package com.ibm.streams.watsonexplorer;

import org.apache.http.auth.Credentials;

public class WEXConnection {

	private String host;
	private int port;
	private Credentials creds;
	
	public WEXConnection(String host, int port, Credentials creds) {
		this.host = host;
		this.port = port;
		this.creds = creds;
	}
	
	public Credentials getCreds() {
		return creds;
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getConnectionUrl() {
		return "http://" + host + ":" + port;
	}
	
}
