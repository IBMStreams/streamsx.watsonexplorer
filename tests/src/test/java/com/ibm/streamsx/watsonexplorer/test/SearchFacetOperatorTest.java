package com.ibm.streamsx.watsonexplorer.test;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Parameter.param;
import static org.mockserver.model.ParameterBody.params;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.watsonexplorer.operators.CASearchFacetOperator;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

public class SearchFacetOperatorTest extends AbstractOperatorTest {

	private static final String QUERY_SEARCH_STRING = "*.*";
	private static final String FACET_SEARCH_STRING = "{\"count\" : 1, \"namespace\":\"keyword\",\"id\":\"$._word.noun\"}";
	private String searchFacetResponseStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		searchFacetResponseStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/searchfacet_response.txt")));
	}
	
	@Test
	public void searchFacetTestGreenThread() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/search/facet")
				.withBody(params(
					param("query", QUERY_SEARCH_STRING),
					param("facet", FACET_SEARCH_STRING),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchFacetResponseStr));

		TStream<String> src = topo.strings(QUERY_SEARCH_STRING);
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLFacetQuery(), Type.Factory.getStreamSchema("tuple<rstring query, rstring facet>"));
		
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, searchFacetResponseStr, CASearchFacetOperator.class);
	}
	
	private static class StringStreamToSPLFacetQuery implements BiFunction<String, OutputTuple, OutputTuple> {
		private static final long serialVersionUID = 1L;

		@Override
		public OutputTuple apply(String query, OutputTuple outTuple) {
			outTuple.setString("query", query);
			outTuple.setString("facet", FACET_SEARCH_STRING);			
			return outTuple;
		}
	}
}
