package com.ibm.streamsx.watsonexplorer;

import java.util.Arrays;
import java.util.HashMap;

public class RestParameters extends HashMap<String, Object> {

	private static final long serialVersionUID = 1L;

	public RestParameters() {
		super();
	}
	
	public RestParameters(RestParameters params) {
		super(params);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(this.entrySet().toArray());
	}
}
