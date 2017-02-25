package com.ibm.streams.watsonexplorer.operators;

import java.util.Set;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.client.EnqueueData.EnqueueDataOptions;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(name = "WatsonExplorerPush", namespace = "com.ibm.streamsx.watsonexplorer", description = WEXPushOperator.DESC)
@InputPorts({
		@InputPortSet(description = "Port that ingests tuples", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Error port. Tuples that failed to be pushed to Watson Explorer are redirected to this port if it is available. The output schema for this port should match the schema for the input port.", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Libraries({ "opt/downloaded/*", "impl/lib/*" })
public class WEXPushOperator extends AbstractWEXOperator {

	private static final String DEFAULT_URL_ATTR_NAME = "url";
	private static final String DEFAULT_DATA_ATTR_NAME = "data";
	private static final String DEFAULT_COLLECTION_ATTR_NAME = "collection";
	private static final String DEFAULT_PROCESING_VALUE = "normal";
	private static final String DEFAULT_DUPLICATES_VALUE = "overwrite";

	private String collection;
	private TupleAttribute<Tuple, String> collectionAttr;
	private TupleAttribute<Tuple, String> urlAttr;
	private TupleAttribute<Tuple, String> dataAttr;
	private String processing = DEFAULT_PROCESING_VALUE;
	private String duplicates = DEFAULT_DUPLICATES_VALUE;

	private RestParameters transientParameters;

	@Parameter(optional = true, description = "Specifies how Watson Explorer should handle pushing data to the collection when"
			+ " the collection already contains a document with the specified URL. Valid options include: <br><br>"
			+ "  * **skip**: If a document with the same URL already exists in the collection, the data will not be pushed. The tuple will be redirected to the error output port, if it exists.</li>"
			+ "  * **overwrite**: If a document with the same URL already exists in the collection, the document will be overwritten with the data being pushed."
			+ "<br><br>"
			+ "By default, this value is set to **overwrite**.")
	public void setDuplicates(String duplicates) {
		this.duplicates = duplicates;
	}

	@Parameter(optional = true, description = "Specifies the name of the collection to push the data to. This parameter should not be set"
			+ " if the **collectionAttr** parameter is specified.")
	public void setCollection(String collection) {
		this.collection = collection;
	}

	@Parameter(optional = true, description = "Specifies the name of the input attribute that contains the collection that the data should be pushed to."
			+ " This parameter should not be set if the **collection** parameter is specified. If this parameter is not specified and the"
			+ " **collection** parameter is not specified, the operator will look for an attributed named *collection*.")
	public void setCollectionAttr(TupleAttribute<Tuple, String> collectionAttr) {
		this.collectionAttr = collectionAttr;
	}

	@Parameter(optional = true, description = "Specifies the input attribute that contains the URL to assign to the document that is stored in the collection."
			+ " The URL should be unique for each tuple. If this parameter is not specified, a unique URL will be generated for each tuple.")
	public void setUrlAttr(TupleAttribute<Tuple, String> urlAttr) {
		this.urlAttr = urlAttr;
	}

	@Parameter(optional = true, description = "Specifies the input attribute that contains the data to be pushed to the collection. If this parameter"
			+ " is not specified, then the operator will look for an input attribute named **data**.")
	public void setDataAttr(TupleAttribute<Tuple, String> dataAttr) {
		this.dataAttr = dataAttr;
	}

	@Parameter(optional = true, description = "Specifies when the Watson Explorer REST API call should return after attempting to push the data"
			+ " to the collection. Valid options include:<br><br>"
			+ "  * **fast**: The data is pushed to the collection and the API returns immediately, without waiting for acknowledgment that the push was successful. If an error occurs when pushing the data, the operator will have no way of knowing or reporting on the error."
			+ "  * **normal**: The data is pushed to the collection and the API waits until it receives a response indicating whether or not the push was successful. In this case, if an error occurs when pushing the data, the operator will redirect the tuple to the error port, if it exists."
			+ "<br><br>"
			+ " The default value for this parameter is *normal*.")
	public void setProcessing(String processing) {
		this.processing = processing;
	}

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		transientParameters = new RestParameters();
	}

	@Override
	protected void initParameters(OperatorContext context) {
		super.initParameters(context);
		if (context.getParameterNames().contains("collection")) {
			addStaticRestParameter("collection", collection);
		}
	}

	
	
	@ContextCheck(compile = true)
	public static void checkParams(OperatorContextChecker checker) {
		checker.checkExcludedParameters("collection", "collectionAttr");

		Set<String> paramNames = checker.getOperatorContext().getParameterNames();
		StreamSchema inputPortSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();

		if (!paramNames.contains("collection") && !paramNames.contains("collectionAttr")) {
			if (inputPortSchema.getAttribute(DEFAULT_COLLECTION_ATTR_NAME) == null) {
				checker.setInvalidContext(
						"Either the 'collection' or 'collectionAttr' parameters must be specified, "
								+ "or an attribute named 'collection' must be present on the input port",
						new Object[0]);
			}
		}

		if (!paramNames.contains("urlAttr")) {
			if (inputPortSchema.getAttribute(DEFAULT_URL_ATTR_NAME) == null) {
				checker.setInvalidContext("Either the 'urlAttr' parameter must be specified or an attribute named"
						+ "'url' must be present on the input port", new Object[0]);
			}
		}

		if (!paramNames.contains("dataAttr")) {
			if (inputPortSchema.getAttribute(DEFAULT_DATA_ATTR_NAME) == null) {
				checker.setInvalidContext("Either the 'dataAttr' parameter must be specified or an attribute named"
						+ "'data' must be present on the input port", new Object[0]);
			}
		}
	}

	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {
		String collectionName = null;
		if (collection == null) {
			collectionName = collectionAttr == null ? tuple.getString(DEFAULT_COLLECTION_ATTR_NAME)
					: collectionAttr.getValue(tuple);
			transientParameters.put("collection", collectionName);
		}

		String data = dataAttr == null ? tuple.getString(DEFAULT_DATA_ATTR_NAME) : dataAttr.getValue(tuple);
		String url = urlAttr == null ? tuple.getString(DEFAULT_URL_ATTR_NAME) : urlAttr.getValue(tuple);
		String synchronization = processing == "fast" ? "none" : "indexed";
		String enqueueType = duplicates == "overwrite" ? "none" : "reenqueued";

		EnqueueDataOptions options = new EnqueueDataOptions();
		options.setData(data);
		options.setUrl(url);
		options.setSynchronization(synchronization);
		options.setEnqueueType(enqueueType);

		boolean isSuccess = wex.enqueueData(options, transientParameters);
		if (!isSuccess) {
			if (getOperatorContext().getStreamingOutputs().size() == 1) {
				StreamingOutput<OutputTuple> outPort = getOutput(0);
				OutputTuple outTuple = getOutput(0).newTuple();
				outTuple.assign(tuple);
				outPort.submit(outTuple);
			}
		}
	}
	
	static final String DESC = "This operator uses the Watson"
			+ " Explorer Foundational Components REST API to push text to the specified collection. Specifically, this operator uses the"
			+ " `search-collection-enqueue` API function to push the data.\\n\\n"
			+ " More information about the available parameters for this API function can be found in the Watson Explorer Knowledge Center:"
			+ " http://www.ibm.com/support/knowledgecenter/SS8NLW_11.0.0/com.ibm.swg.im.infosphere.dataexpl.engine.srapi.man.doc/r_function_search-collection-enqueue.html";
}
