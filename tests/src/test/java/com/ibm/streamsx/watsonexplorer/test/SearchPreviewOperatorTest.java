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
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.watsonexplorer.operators.CASearchPreviewOperator;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

public class SearchPreviewOperatorTest extends AbstractOperatorTest {

	private static final String URI_SEARCH_STRING = "csv://sample_data?id=1";
	private static final String QUERY_SEARCH_STRING = "search";
	private static final StreamSchema URI_SCHEMA = Type.Factory.getStreamSchema("tuple<rstring uri, rstring query>");
	private String searchPreviewResponseStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		searchPreviewResponseStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/searchpreview_response.txt")));
	}
	
	@Test
	public void searchPreviewTestGreenThread() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/search/preview")
				.withBody(params(
					param("uri", URI_SEARCH_STRING),
					param("query", QUERY_SEARCH_STRING)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchPreviewResponseStr));

		TStream<String> src = topo.strings(URI_SEARCH_STRING);
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPL(), URI_SCHEMA);

		opParams.put("outputFormat", "application/json");
		opParams.put("uriAttr", URI_SCHEMA.getAttribute(0));
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, searchPreviewResponseStr, CASearchPreviewOperator.class);
	}
	
	private static class StringStreamToSPL implements BiFunction<String, OutputTuple, OutputTuple> {
		private static final long serialVersionUID = 1L;

		@Override
		public OutputTuple apply(String uri, OutputTuple outTuple) {
			outTuple.setString("uri", uri);
			outTuple.setString("query", QUERY_SEARCH_STRING);
			return outTuple;
		}
	}
}
