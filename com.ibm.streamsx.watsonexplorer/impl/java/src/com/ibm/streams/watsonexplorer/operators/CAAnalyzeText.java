/* Generated by Streams Studio: November 30, 2016 at 10:33:19 AM EST */
package com.ibm.streams.watsonexplorer.operators;

import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.SearchResult;

@PrimitiveOperator(name = "AnalyzeText", namespace = "com.ibm.streamsx.watsonexplorer", description = CAAnalyzeText.DESC)
@OutputPorts({
	@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating)})
public class CAAnalyzeText extends AbstractCAOperator {

	private static final String DEFAULT_TEXT_ATTR_NAME = "text";
	
	private TupleAttribute<Tuple, String> textAttr;
	
	private Logger logger = Logger.getLogger(CAAnalyzeText.class);

	@Parameter(optional = true, description = "Specifies the name of the input attribute that contains the text to be analyzed."
			+ " If this parameter is not specified, the operator will look for an attribute named *text*.")
	public void setTextAttr(TupleAttribute<Tuple, String> textAttr) {
		this.textAttr = textAttr;
	}
	
	public TupleAttribute<Tuple, String> getTextAttr() {
		return textAttr;
	}

	@ContextCheck(compile = true)
	public static void checkParams(OperatorContextChecker checker) {
		Set<String> paramNames = checker.getOperatorContext().getParameterNames();

		if (!paramNames.contains("textAttr")) {
			StreamSchema inputPortSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
			if (inputPortSchema.getAttribute("text") == null) {
				checker.setInvalidContext("Either the 'textAttr' parameter must be specified, "
						+ "or an attribute named 'text' must be present on the input port", new Object[0]);
			}
		}
	}
	
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {
		StreamingOutput<OutputTuple> outStream = getOutput(0);
		OutputTuple outTuple = outStream.newTuple();
		outTuple.assign(tuple);
		
		String text = textAttr == null ? tuple.getString(DEFAULT_TEXT_ATTR_NAME) : textAttr.getValue(tuple);
		logger.trace("text=" + text);

		RestParameters params = new RestParameters();
		params.put("text", text);
		
		// check if collection name is being passed in as a tuple attribute
		if(!getOperatorContext().getParameterNames().contains("collectionName")) {
			addCollectionName(params, tuple);
		}
		
		SearchResult result = caClient.analyzeText(params);

		outTuple.setString(getResultAttrName(), result.getContent());
		outStream.submit(outTuple);
	}
	
	static final String DESC = "This operator uses the Watson"
			+ " Explorer Content Analytics `/analysis/text` REST API to provide real-time natural language processing (NLP) against ad-hoc text."
			+ " The operator ingests a string, sends the text to the specified collection for analysis and returns the result set. The"
			+ " specific text analysis that is performed depends on the annotators that are present in the specified collection.\\n\\n"
			+ " More information about the available parameters for this REST API can found in the Content Analytics REST API documentation in"
			+ " the Content Analytics installation directory: `<ES_INSTALL_PATH>/docs/api/rest/search`.";
}
