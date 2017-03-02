package com.ibm.streamsx.watsonexplorer.test;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Parameter.param;
import static org.mockserver.model.ParameterBody.params;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;
import org.mockserver.model.NottableString;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.watsonexplorer.operators.WEXQueryOperator;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

public class WatsonExplorerQueryTest extends AbstractOperatorTest {
	
	private String wexQueryResponseXMLStr;
	private String wexQueryResponsePage1XMLStr;
	private String wexQueryResponsePage2XMLStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		wexQueryResponseXMLStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/wexquery_response_greenthread.txt")));
		wexQueryResponsePage1XMLStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/wexquery_response_page1.txt")));
		wexQueryResponsePage2XMLStr = new String(Files.readAllBytes(Paths.get("src/test/resources/responses/wexquery_response_page2.txt")));
	}
	
	@Override
	protected void initClient() {
	}

	@Test
	public void wexQueryGreenthread() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("sources", "BBCNews"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "query-search"),
					param("query", "car")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexQueryResponseXMLStr));

		TStream<String> src = topo.strings("car");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("username", "guest");
		opParams.put("password", "guest");
		opParams.put("sources", "BBCNews");
		runTopology(splStream, wexQueryResponseXMLStr, WEXQueryOperator.class);
	}

	@Test
	public void wexQueryBrowse() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("sources", "BBCNews"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "query-search"),
					param("query", "car"),
					param("browse", "true"),
					param("browse-num", "5"),
					param(NottableString.not("browse-start"), NottableString.string("5")),
					param("num", "10")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexQueryResponsePage1XMLStr));

		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("sources", "BBCNews"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "query-search"),
					param("query", "car"),
					param("browse", "true"),
					param("num", "10"),
					param("browse-start", "5")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexQueryResponsePage2XMLStr));
		
		TStream<String> src = topo.strings("car");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("username", "guest");
		opParams.put("password", "guest");
		opParams.put("sources", "BBCNews");
		opParams.put("browse", true);
		opParams.put("browseNum", 5);
		opParams.put("num", 10);

		runTopology(splStream, new String[]{wexQueryResponsePage1XMLStr, wexQueryResponsePage2XMLStr}, WEXQueryOperator.class);
	}	
	
	@Test
	public void wexQueryAdditionalParams() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("sources", "BBCNews"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "query-search"),
					param("foo", "123"),
					param("bar", "987"),
					param("query", "car")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexQueryResponseXMLStr));

		TStream<String> src = topo.strings("car");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring query>"));
		
		opParams.put("username", "guest");
		opParams.put("password", "guest");
		opParams.put("sources", "BBCNews");
		opParams.put("additionalParams", new String[] {"foo=123", "bar=987"});
		runTopology(splStream, wexQueryResponseXMLStr, WEXQueryOperator.class);
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
