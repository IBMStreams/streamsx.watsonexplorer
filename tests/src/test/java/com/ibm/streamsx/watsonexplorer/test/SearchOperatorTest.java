package com.ibm.streamsx.watsonexplorer.test;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Parameter.param;
import static org.mockserver.model.ParameterBody.params;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;

import org.junit.Before;
import org.junit.Test;
import org.mockserver.model.NottableString;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.watsonexplorer.operators.CASearchOperator;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

public class SearchOperatorTest extends AbstractOperatorTest {
	
	private String searchResponseJSONStr;
	private String searchResponseXMLStr;
	private String searchResponseAtomXMLStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		searchResponseJSONStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/search_response_json.txt")));
		searchResponseXMLStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/search_response_xml.txt")));
		searchResponseAtomXMLStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/search_response_atomxml.txt")));
	}
	

	@Test
	public void searchOpTestGreenthread() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/search")
				.withBody(params(
					param("query", "abc"),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchResponseAtomXMLStr));

		TStream<String> src = topo.strings("abc");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, searchResponseAtomXMLStr, CASearchOperator.class);
	}
	
	@Test
	public void searchOpTestXMLResponse() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/search")
				.withBody(params(
					param("query", "abc"),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchResponseXMLStr));

		TStream<String> src = topo.strings("abc");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("outputFormat", "application/xml");
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, searchResponseXMLStr, CASearchOperator.class);		
	}
	
	@Test
	public void searchOpTestJsonResponse() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/search")
				.withBody(params(
					param("query", "abc"),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchResponseJSONStr));

		TStream<String> src = topo.strings("abc");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("outputFormat", "application/json");
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, searchResponseJSONStr, CASearchOperator.class);		
	}
	
	@Test
	public void searchOpTestWithAuth() throws Exception {
		String encodedCreds = "Basic " + Base64.getEncoder().encodeToString(("test:test").getBytes());
		
		client
			.when(request()
					.withMethod("POST")
					.withPath("/api/v10/search")
					.withHeader(NottableString.not("Authorization"), NottableString.not(encodedCreds))
			)
			.respond(response()
					.withStatusCode(401)
					.withHeader("WWW-Authenticate", "Basic realm=\"My server\"")
					.withHeader("Content-Length", "0")
			);
		
		client
		.when(request()
				.withHeader("Authorization", encodedCreds)
				.withMethod("POST")
				.withPath("/api/v10/search")
				.withBody(params(
					param("query", "abc"),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchResponseAtomXMLStr));
		
		TStream<String> src = topo.strings("abc");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("username", "test");
		opParams.put("password", "test");
		opParams.put("collectionName", COLLECTION_NAME);
	
		runTopology(splStream, searchResponseAtomXMLStr, CASearchOperator.class);
	}
	
	@Test
	public void searchOpTestAdditionalParams() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/api/v10/search")
				.withBody(params(
					param("query", "abc"),
					param("foo", "12345"),
					param("bar", "98765"),
					param("collection", COLLECTION_ID)
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(searchResponseAtomXMLStr));

		TStream<String> src = topo.strings("abc");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("collectionName", COLLECTION_NAME);
		opParams.put("additionalParams", Arrays.asList("foo=12345", "bar=98765").toArray());
	
		runTopology(splStream, searchResponseAtomXMLStr, CASearchOperator.class);
	}

	private static class StringStreamToSPLQuery implements BiFunction<String, OutputTuple, OutputTuple> {
		private static final long serialVersionUID = 1L;

		@Override
		public OutputTuple apply(String query, OutputTuple outTuple) {
			outTuple.setString("query", query);
			return outTuple;
		}
	}
}
