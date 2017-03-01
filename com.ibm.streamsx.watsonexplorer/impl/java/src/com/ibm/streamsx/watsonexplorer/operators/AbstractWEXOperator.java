package com.ibm.streamsx.watsonexplorer.operators;

import java.util.List;

import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.watsonexplorer.RestParameters;
import com.ibm.streamsx.watsonexplorer.WEXConnection;
import com.ibm.streamsx.watsonexplorer.client.WatsonExplorer;

@Libraries({"opt/downloaded/*", "impl/lib/*"})
public class AbstractWEXOperator extends AbstractOperator {

	private static final String DEFAULT_RESULT_ATTR_NAME = "result";
	
	protected WatsonExplorer wex;

	protected String host;
	protected Integer port;
	protected String username;
	protected String password;
	protected String resultAttributeName = DEFAULT_RESULT_ATTR_NAME;
	protected List<String> additionalParams;
	protected WEXConnection connection;
	protected RestParameters staticParameters;
	
	private Logger logger = Logger.getLogger(AbstractWEXOperator.class);
	
	@Parameter(optional = true, name = "resultAttrName", description = "Specifies the name of the output attribute that should be populated with the results."
			+ " The output attribute type should be *rstring*. If this parameter is not specified, the operator will look for an"
			+ " output attribute named *result*.")
	public void setResultAttributeName(String resultAttributeName) {
		this.resultAttributeName = resultAttributeName;
	}
	
	@Parameter(optional = false, description = "Specifies the name of the host to use when executing the REST API calls.")
	public void setHost(String host) {
		this.host = host;
	}

	@Parameter(optional = false, description = "Specifies the port number to use when executing the REST API calls.")
	public void setPort(Integer port) {
		this.port = port;
	}

	@Parameter(optional = false, description = "Specifies the username to use when executing the REST API calls.")
	public void setUsername(String username) {
		this.username = username;
	}

	@Parameter(optional = false, description = "Specifies the password to use when executing the REST API calls.")
	public void setPassword(String password) {
		this.password = password;
	}

	@Parameter(optional = true, description = "Allows for specifying a list of additional parameters to include with the REST API call. Each additional"
			+ " parameter to be written using the format: <name>=<value>.")
	public void setAdditionalParams(List<String> additionalParams) {
		this.additionalParams = additionalParams;
	}
	
	public String getResultAttributeName() {
		return resultAttributeName;
	}
	
	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
	
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: "
				+ context.getPE().getJobId());

		staticParameters = new RestParameters();
		Credentials creds = new UsernamePasswordCredentials(getUsername(), getPassword());
		connection = new WEXConnection(getHost(), getPort(), creds);
		
		initParameters(context);
		wex = new WatsonExplorer(connection, staticParameters);
	}
	
	protected void initParameters(OperatorContext context) {
		// add parameters		
		if(additionalParams != null) {
			additionalParams.forEach(kv -> {
				String[] kvPair = kv.split("=", 2);
				if(kvPair.length == 2) {
					addStaticRestParameter(kvPair[0], kvPair[1]);
				}
			});
		}
	}
	
	@Override
	public synchronized void allPortsReady() throws Exception {
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId()
				+ " in Job: " + context.getPE().getJobId());
		
		logger.trace("parameters=" + staticParameters.toString());
	}
	
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
		// For window markers, punctuate all output ports
		super.processPunctuation(stream, mark);
	}

	public synchronized void shutdown() throws Exception {
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId()
				+ " in Job: " + context.getPE().getJobId());

		// Must call super.shutdown()
		super.shutdown();
	}
	
	protected void addStaticRestParameter(String key, Object value) {
		staticParameters.put(key, value);
	}
}
