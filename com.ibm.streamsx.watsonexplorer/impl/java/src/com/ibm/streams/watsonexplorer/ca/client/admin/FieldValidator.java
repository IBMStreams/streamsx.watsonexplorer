package com.ibm.streams.watsonexplorer.ca.client.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.WEXConnection;
import com.ibm.streams.watsonexplorer.ca.client.CollectionsUtil;
import com.ibm.streams.watsonexplorer.ca.client.ContentAnalytics;

public class FieldValidator {

	private static Map<String /* collectionName */, List<String> /* missing fields */> missingFieldCache = new HashMap<String, List<String>>();
	private static Gson gson = new Gson();
	
	/**
	 * Checks if a collection contains the specified fields. If all of the fields are defined
	 * in the collection, then an empty list is returned. If any fields are missing, a list 
	 * containing the missing fields is returned.  
	 * @param fieldsToCheck the list of field names to check
	 * @return A list containing field names that were not found, or an empty list if all fields are defined in the collection
	 */
	public static List<String> collectionHasFields(WEXConnection connection, List<String> fieldsToCheck, String collectionName) throws Exception {
		if(missingFieldCache.containsKey(collectionName)) {
			return missingFieldCache.get(collectionName);
		}
		
		String collectionId = CollectionsUtil.getCollectionId(connection, collectionName);		
		ContentAnalytics ca = new ContentAnalytics(connection);
		RestParameters params = new RestParameters();
		params.put("collection", collectionId);
		params.put("output", "application/json");
		String jsonStr = ca.adminGetFields(params);
		
		List<String> collectionFields = new ArrayList<String>();
		
		// parse the jsonStr
		JsonObject jsonObj = gson.fromJson(jsonStr, JsonObject.class);
		if(jsonObj.has("es_apiResponse")) {
			JsonObject respObj = jsonObj.getAsJsonObject("es_apiResponse");
			if(respObj.has("es_indexFields")) {
				JsonObject fieldsObj = respObj.getAsJsonObject("es_indexFields");
				if(fieldsObj.has("es_indexField")) {
					JsonArray fieldsArr = fieldsObj.get("es_indexField").getAsJsonArray();
					for(int i = 0; i < fieldsArr.size(); i++) {
						JsonObject field = fieldsArr.get(i).getAsJsonObject();
						if(field.has("name")) {
							collectionFields.add(field.get("name").getAsString());
						}
					}
				}
			}
		}
		
		List<String> resultList = new ArrayList<String>(fieldsToCheck);		
		resultList.removeAll(collectionFields); // remaining field names are not defined in collection
		missingFieldCache.put("collectionName", resultList);
		
		return resultList; 
	}
}
