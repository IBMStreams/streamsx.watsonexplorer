package com.ibm.streams.watsonexplorer.operators;

import java.util.Set;

import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@OutputPorts({
	@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating)})
public class AbstractSearchOperator extends AbstractCAOperator {

	protected static final String DEFAULT_QUERY_ATTR_NAME = "query";
	
	protected TupleAttribute<Tuple, String> queryAttr;
	protected String query;

	@Parameter(optional = true, description = "Specifies the name of the input attribute that contains the query to be executed." 
			+ " This parameter should not be set if the **query** parameter is specified."
			+ " If this parameter is not specified and the **query** parameter is not specified,"
			+ " the operator will look for an attribute named *query*.")
	public void setQueryAttr(TupleAttribute<Tuple, String> queryAttr) {
		this.queryAttr = queryAttr;
	}

	@Parameter(optional = true, description = "Specifies the query that should be executed. This parameter should not be set if"
			+ " the **query** parameter is specified.")
	public void setQuery(String query) {
		this.query = query;
	}

	public TupleAttribute<Tuple, String> getQueryAttr() {
		return queryAttr;
	}

	public String getQuery() {
		return query;
	}

	@ContextCheck(compile = true)
	public static void checkParams(OperatorContextChecker checker) {
		checker.checkExcludedParameters("query", "queryAttr");
		
		Set<String> paramNames = checker.getOperatorContext().getParameterNames();

		if (!paramNames.contains("query") && !paramNames.contains("queryAttr")) {
			StreamSchema inputPortSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
			if (inputPortSchema.getAttribute("query") == null) {
				checker.setInvalidContext("Either the 'query' or 'queryAttr' parameters must be specified, "
						+ "or an attribute named 'query' must be present on the input port", new Object[0]);
			}
		}
	}
	
}
