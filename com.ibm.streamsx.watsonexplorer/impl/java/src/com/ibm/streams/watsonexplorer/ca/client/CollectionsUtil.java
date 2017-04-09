package com.ibm.streams.watsonexplorer.ca.client;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streams.watsonexplorer.RestParameters;
import com.ibm.streams.watsonexplorer.WEXConnection;

public class CollectionsUtil {

	private static Map<String /* collectionName */, String /* collectionId */> collectionNameCache = new HashMap<String, String>();

	/*
	 * Parses the collection response and returns the collection ID if found, otherwise returns null.
	 */
	private static String parseCollectionResponse(String content, String collectionName) {
		Gson gson = new Gson();
		JsonObject jsonObj = gson.fromJson(content, JsonObject.class);
		if(jsonObj.has("es_apiResponse")) {
			JsonObject apiObj = jsonObj.getAsJsonObject("es_apiResponse");
			if(apiObj.has("es_collection")) {
				JsonElement jsonElement = apiObj.get("es_collection");
				if(jsonElement.isJsonArray()) {
					JsonArray collectionArr = jsonElement.getAsJsonArray();
					for(JsonElement collection : collectionArr) {
						JsonObject collectionObj = collection.getAsJsonObject();
						if(collectionObj.has("label") && collectionObj.has("id")) {
							if(collectionName.equals(collectionObj.get("label").getAsString())) {
								return collectionObj.get("id").getAsString();
							}
						}
					}
				}
			}
		}
		
		return null;
	}

	/**
	 * Retrieves the collection ID using the standard REST API
	 * @param connection
	 * @param collectionName
	 * @return The collection ID for the given collectionName
	 * @throws Exception
	 */
	public static String getCollectionId(WEXConnection connection, String collectionName) throws Exception {
		return getCollectionId(connection, collectionName, false);
	}
	
	/**
	 * Retrieves the collection ID using either the standard REST API or the Admin API
	 * @param connection
	 * @param collectionName
	 * @param useAdminAPI
	 * @return The collection ID for the given collectionName
	 * @throws Exception
	 */
	public static String getCollectionId(WEXConnection connection, String collectionName, boolean useAdminAPI) throws Exception {
		// return the ID in the cache if it exists
		if(collectionNameCache.containsKey(collectionName))
			return collectionNameCache.get(collectionName);
		
		ContentAnalytics ca = new ContentAnalytics(connection);
		String content = ca.collections(new RestParameters(), useAdminAPI);

		String collectionId = parseCollectionResponse(content, collectionName);
		if(collectionId == null) {
			throw new Exception("Unable to find collection ID for collection name: " + collectionName);
		}
		
		collectionNameCache.put(collectionName, collectionId);
		
		return collectionId;
	}
}
