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
import com.ibm.streams.watsonexplorer.operators.CAAnalyzeText;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

public class AnalyzeTextOperatorTest extends AbstractOperatorTest {

	private static final String INPUT_TEXT = "My car is broken";
	private static final StreamSchema TEXT_SCHEMA = Type.Factory.getStreamSchema("tuple<rstring text>");
	private String analyzeTextResponseStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		analyzeTextResponseStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/analyzetext_response.txt")));
	}
	
	@Test
	public void analyzeTextTestGreenThread() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/analysis/text")
				.withBody(params(
					param("text", INPUT_TEXT),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(analyzeTextResponseStr));

		TStream<String> src = topo.strings(INPUT_TEXT);
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPL(), TEXT_SCHEMA);

		opParams.put("textAttr", TEXT_SCHEMA.getAttribute(0));
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, analyzeTextResponseStr, CAAnalyzeText.class);
	}
	
	private static class StringStreamToSPL implements BiFunction<String, OutputTuple, OutputTuple> {
		private static final long serialVersionUID = 1L;

		@Override
		public OutputTuple apply(String text, OutputTuple outTuple) {
			outTuple.setString("text", text);
			return outTuple;
		}
	}
}
