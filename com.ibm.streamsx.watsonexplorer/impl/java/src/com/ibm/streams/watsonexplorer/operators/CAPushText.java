package com.ibm.streams.watsonexplorer.operators;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.ca.client.CollectionsUtil;
import com.ibm.streams.watsonexplorer.ca.client.admin.FieldValidator;

@PrimitiveOperator(name = "PushText", namespace = "com.ibm.streamsx.watsonexplorer", description = CAPushText.DESC)
public class CAPushText extends AbstractCAOperator {

	public enum MissingFieldPolicy {
		Ignore,
		Warning,
		Error
	};
	
	private static final String DEFAULT_TEXT_ATTR_NAME = "text";
	private static final String DEFAULT_DOC_ID_ATTR_NAME = "docId";
	private static final MissingFieldPolicy DEFAULT_MISSING_FIELD_POLICY = MissingFieldPolicy.Error;
	
	private Logger logger = Logger.getLogger(CAPushText.class);
	private Gson gson;
	
	private TupleAttribute<Tuple, String> textAttr;
	private TupleAttribute<Tuple, String> documentIdAttr;
	private List<String> fieldAttributeNames;
//	private List<TupleAttribute<Tuple, String>> fieldAttributeNames;
	private MissingFieldPolicy missingFieldPolicy = DEFAULT_MISSING_FIELD_POLICY;
	
	@Parameter(optional = false, description = "Specifies the username to use when executing the REST API calls.")
	public void setUsername(String username) {
		super.setUsername(username);
	}

	@Parameter(optional = false, description = "Specifies the password to use when executing the REST API calls.")
	public void setPassword(String password) {
		super.setPassword(password);
	}
	
	@Parameter(optional = true, description = "Specifies the policy to apply if the mapped fields are missing. This parameter is "
			+ "ignored if the **fieldAttributeNames** parameter is not specified. The following policy options are available: "
			+ "<br /><br /><ul><li>**Ignore** - If this policy is set, the operator will not check if the fields exist prior to "
			+ "pushing the text content. <li>**Warning** - If this policy is set, the operator will check if the fields exist prior "
			+ "to pushing the text content and log a warning if any fields are missing. <li>**Error** - If this policy is set, the "
			+ "operator will check if the fields exist prior to pushing the text content. If any fields are missing the operator will "
			+ "throw an exception and terminate.</ul>")
	public void setMissingFieldPolicy(MissingFieldPolicy missingFieldPolicy) {
		this.missingFieldPolicy = missingFieldPolicy;
	}
	
	public MissingFieldPolicy getMissingFieldPolicy() {
		return missingFieldPolicy;
	}
	
	@Parameter(optional = true, cardinality = -1, description = "Specifies a list of attributes that should be mapped to index fields when pushing the "
			+ "text content to the collection. The field names will be set based on the names of the attributes included in the list "
			+ "(i.e. attribute firstName will be mapped to an index field called 'firstName').")
	public void setFieldAttributeNames(List<String> fieldAttributeNames) {
		this.fieldAttributeNames = fieldAttributeNames;
	}
	
	public List<String> getFieldAttributeNames() {
		return fieldAttributeNames;
	}
	
	@Parameter(optional = true, description = "Specifies the name of the input attribute that contains the text to be added to the collection."
			+ " If this parameter is not specified, the operator will look for an attribute named *text*.")
	public void setTextAttr(TupleAttribute<Tuple, String> textAttr) {
		this.textAttr = textAttr;
	}
	
	public TupleAttribute<Tuple, String> getTextAttr() {
		return textAttr;
	}

	@Parameter(optional = true, description = "Specifies the name of the input attribute that contains the document ID."
			+ " If this paraemter is not specified, the operator will look for an attribute named *docId*.")
	public void setDocumentIdAttr(TupleAttribute<Tuple, String> documentIdAttr) {
		this.documentIdAttr = documentIdAttr;
	}
	
	public TupleAttribute<Tuple, String> getDocumentIdAttr() {
		return documentIdAttr;
	}
	
	@ContextCheck(compile = false, runtime = true)
	public static void checkFieldAttributeName(OperatorContextChecker checker) {
		Set<String> paramNames = checker.getOperatorContext().getParameterNames();
		Set<String> inputSchemaAttributes = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema().getAttributeNames();
		
		if(paramNames.contains("fieldAttributeNames")) {
			List<String> attributeNames = checker.getOperatorContext().getParameterValues("fieldAttributeNames");
			for(String attrName : attributeNames) {
				if(!inputSchemaAttributes.contains(attrName)) {
					checker.setInvalidContext("Input schema does not contain attribute with name: '" + attrName + "'", new Object[0]);
				}
			}
		}
	}
	
	@ContextCheck(compile = true)
	public static void checkParams(OperatorContextChecker checker) {
		Set<String> paramNames = checker.getOperatorContext().getParameterNames();

		if (!paramNames.contains("textAttr")) {
			StreamSchema inputPortSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
			if (inputPortSchema.getAttribute(DEFAULT_TEXT_ATTR_NAME) == null) {
				checker.setInvalidContext("Either the 'textAttr' parameter must be specified, "
						+ "or an attribute named 'text' must be present on the input port", new Object[0]);
			}
		}
		
		if(!paramNames.contains("documentIdAttr")) {
			StreamSchema inputPortSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
			if (inputPortSchema.getAttribute(DEFAULT_DOC_ID_ATTR_NAME) == null) {
				checker.setInvalidContext("Either the 'docIdAttr' parameter must be specified, "
						+ "or an attribute named 'docId' must be present on the input port", new Object[0]);
			}
		}
	}
	
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		gson = new Gson();
		
		// apply the missing field policy to the static collection name
		if(context.getParameterNames().contains("collectionName")) {
			applyMissingFieldPolicy(getCollectionName());
		}
	}
	
	/*
	 * Applies the missing field policy.
	 */
	private void applyMissingFieldPolicy(String collectionName) throws Exception {
		MissingFieldPolicy policy = getMissingFieldPolicy();
		if(policy == MissingFieldPolicy.Ignore) {
			return;
		} else {
			// get all attribute names specified by 'fieldAttributeNames'
			List<String> fieldsToCheck = getFieldAttributeNames();
			if(fieldsToCheck == null || fieldsToCheck.isEmpty())
				return;
			
			List<String> missingFields = FieldValidator.collectionHasFields(connection, fieldsToCheck, collectionName);
			logger.trace("# of missing fields: " + missingFields.size());
			
			if(missingFields.size() > 0) {
				StringBuilder sb = new StringBuilder();
				for(int i = 0; i < missingFields.size(); i++) {
					sb.append(missingFields.get(i));
					
					if(i+1 < missingFields.size())
						sb.append(", ");
				}
				String msg = "[MissingFieldPolicy=" + policy.name() + "] The following fields are not defined in the collection: " + sb.toString();
				if(policy == MissingFieldPolicy.Warning) {
					logger.warn(msg);
				} else if(policy == MissingFieldPolicy.Error) {
					logger.error(msg);
					throw new Exception(msg);
				}
			}
		}
	}

	@Override
	protected String getCollectionId(String collectionName) throws Exception {
		return CollectionsUtil.getCollectionId(connection, collectionName, true);
	}
	
	private String formatFieldsString(Tuple tuple) {
		JsonObject jsonObj = new JsonObject();
		
		List<String> fieldAttrNames = getFieldAttributeNames();
		if(fieldAttrNames != null && !fieldAttrNames.isEmpty()) {
			for(String attrName : fieldAttrNames) {
				String fieldName = attrName;
				String fieldValue = tuple.getObject(attrName).toString();
				
				jsonObj.addProperty(fieldName, fieldValue);
			}	
		}
		
		logger.trace("fields string=" + jsonObj.toString());
		return gson.toJson(jsonObj);
	}
	
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {
		RestParameters params = new RestParameters();
	
		// check if collection name is being passed in as a tuple attribute
		if(!getOperatorContext().getParameterNames().contains("collectionName")) {
			// apply the missing field policy to the dynamic collection name
			applyMissingFieldPolicy(getDynamicCollectionName(tuple));
			
			// add the collection name to the rest parameters
			addCollectionName(params, tuple);
		}
		
		String text = textAttr == null ? tuple.getString(DEFAULT_TEXT_ATTR_NAME) : textAttr.getValue(tuple);
		logger.trace("text=" + text);

		String docId = documentIdAttr == null ? tuple.getString(DEFAULT_DOC_ID_ATTR_NAME) : documentIdAttr.getValue(tuple);
		logger.trace("docId=" + docId);
		
		params.put("text", text);
		params.put("documentId", docId);
		params.put("fields", formatFieldsString(tuple));
		
		// add the text -- client code handles login
		caClient.adminAddText(params);
	}

	@Override
	public synchronized void shutdown() throws Exception {
		super.shutdown();
		
		caClient.adminLogout();
	}
	
	static final String DESC = "";
}
