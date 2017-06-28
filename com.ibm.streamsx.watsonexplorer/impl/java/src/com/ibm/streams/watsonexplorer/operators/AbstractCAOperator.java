package com.ibm.streams.watsonexplorer.operators;

import java.util.List;
import java.util.Set;

import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.WEXConnection;
import com.ibm.streams.watsonexplorer.ca.client.CollectionsUtil;
import com.ibm.streams.watsonexplorer.ca.client.ContentAnalytics;

@InputPorts({
		@InputPortSet(description = "Port that ingests tuples", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious)})
@Libraries({"opt/downloaded/*", "impl/lib/*"})
public class AbstractCAOperator extends AbstractOperator {

	private static final String DEFAULT_RESULT_ATTR_NAME = "result";
	
	protected String host;
	protected Integer port;
	protected String username;
	protected String password;
	protected TupleAttribute<Tuple, String> collectionNameAttr;
	protected String collectionName;
	protected String outputFormat;
	protected String resultAttrName = DEFAULT_RESULT_ATTR_NAME;
	protected List<String> additionalParams;

	private Logger logger = Logger.getLogger(CASearchOperator.class);
	protected ContentAnalytics caClient;
	protected WEXConnection connection;
	private RestParameters staticParameters;

	@Parameter(optional = true, description = "Specifies the name of the input attribute that contains the collection that the REST API call"
			+ " should run against. This parameter cannot be set if the **collectionName** parameter is specified.")
	public void setCollectionNameAttr(TupleAttribute<Tuple, String> collectionNameAttr) {
		this.collectionNameAttr = collectionNameAttr;
	}
	
	@Parameter(optional = true, description = "Allows for specifying a list of additional parameters to include with the REST API call. Each additional"
			+ " parameter to be written using the format: <name>=<value>.")
	public void setAdditionalParams(List<String> additionalParams) {
		this.additionalParams = additionalParams;
	}
	
	@Parameter(optional = false, description = "Specifies the name of the host to use when executing the REST API calls.")
	public void setHost(String host) {
		this.host = host;
	}

	@Parameter(optional = false, description = "Specifies the port number to use when executing the REST API calls.")
	public void setPort(Integer port) {
		this.port = port;
	}

	@Parameter(optional = true, description = "Specifies the username to use when executing the REST API calls.")
	public void setUsername(String username) {
		this.username = username;
	}

	@Parameter(optional = true, description = "Specifies the password to use when executing the REST API calls.")
	public void setPassword(String password) {
		this.password = password;
	}

	@Parameter(optional = true, description = "Specifies the name of the collection that the REST API call"
			+ " should run against. This parameter cannot be set if the **collectionNameAttr** parameter is specified.")
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	@Parameter(optional = true, description = "Specifies the output format that the results should be returned as. By default,"
			+ " most REST API calls will return the results in XML. Valid values depend on the specific REST API call being made, however"
			+ " `application/xml` and `application/json` are generally supported by all REST API calls.")
	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Parameter(optional = true, description = "Specifies the name of the output attribute that should be populated with the results."
			+ " The output attribute type should be *rstring*. If this parameter is not specified, the operator will look for an"
			+ " output attribute named *result*.")
	public void setResultAttrName(String resultAttrName) {
		this.resultAttrName = resultAttrName;
	}

	public TupleAttribute<Tuple, String> getCollectionNameAttr() {
		return collectionNameAttr;
	}
	
	public List<String> getAdditionalParams() {
		return additionalParams;
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

	public String getCollectionName() {
		return collectionName;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public String getResultAttrName() {
		return resultAttrName;
	}
	
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		logger.trace("Operator " + context.getName() + " initializing in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
		
		Credentials creds = null;
		if(context.getParameterNames().contains("username") && context.getParameterNames().contains("password"))
			creds = new UsernamePasswordCredentials(getUsername(), getPassword());

		connection = new WEXConnection(host, port, creds);
		staticParameters = new RestParameters();
		
		initParameters(context);
		caClient = new ContentAnalytics(connection, staticParameters);
		logger.trace("static_parameters=" + staticParameters.toString());
	}
	
	protected void initParameters(OperatorContext context) throws Exception {
		
		if(context.getParameterNames().contains("collectionName")) {
			String collectionId = getCollectionId(collectionName);
			if(collectionId == null) {
				throw new Exception("Unable to find collection with name: " + collectionName);
			}
			addStaticRestParameter("collection", collectionId);	
		}
		
		if(context.getParameterNames().contains("outputFormat"))
			addStaticRestParameter("output", getOutputFormat());
		
		if(additionalParams != null) {
			additionalParams.forEach(kv -> {
				String[] kvPair = kv.split("=", 2);
				if(kvPair.length == 2) {
					addStaticRestParameter(kvPair[0], kvPair[1]);
				}
			});
		}
	}

	protected String getCollectionId(String collectionName) throws Exception {
		return CollectionsUtil.getCollectionId(connection, collectionName);
	}
	
	@ContextCheck(compile = true)
	public static void checkParams(OperatorContextChecker checker) {
		checker.checkExcludedParameters("collectionName", "collectionNameAttr");
		
		Set<String> paramNames = checker.getOperatorContext().getParameterNames();

		if (!paramNames.contains("collectionName") && !paramNames.contains("collectionNameAttr")) {
			StreamSchema inputPortSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
			if (inputPortSchema.getAttribute("collectionName") == null) {
				checker.setInvalidContext("Either the 'collectionName' or 'collectionNameAttr' parameters must be specified, "
						+ "or an attribute named 'collectionName' must be present on the input port", new Object[0]);
			}
		}
	}
	
	@Override
	public synchronized void allPortsReady() throws Exception {
		// This method is commonly used by source operators.
		// Operators that process incoming tuples generally do not need this
		// notification.
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName() + " all ports are ready in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
	}	

	protected String getDynamicCollectionName(Tuple tuple) throws Exception {
		String collectionName;
		if(getCollectionNameAttr() != null) {
			collectionName = getCollectionNameAttr().getValue(tuple);
		} else if(tuple.getStreamSchema().getAttribute("collectionName") != null) {
			collectionName = tuple.getString("collectionName");
		} else {
			throw new Exception("Either the 'collectionName' or 'collectionNameAttr' parameters must be specified, "
					+ "or an attribute named 'collectionName' must be present on the input port"); // should never here
		}
		
		return collectionName;
	}
	
	protected void addCollectionName(RestParameters params, Tuple tuple) throws Exception {
		String collectionName = getDynamicCollectionName(tuple);
 		
		String collectionId = getCollectionId(collectionName);
		if(collectionId == null) {
			throw new Exception("Unable to find collection with name: " + collectionName);
		}
		
		params.put("collection", collectionId);
	}
	
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
		// For window markers, punctuate all output ports
		super.processPunctuation(stream, mark);
	}

	public synchronized void shutdown() throws Exception {
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName() + " shutting down in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

		// TODO: If needed, close connections or release resources related to
		// any external system or data store.

		// Must call super.shutdown()
		super.shutdown();
	}	
	
	protected void addStaticRestParameter(String key, String value) {
		staticParameters.put(key, value);
	}
}
