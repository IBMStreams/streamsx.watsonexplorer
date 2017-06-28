package com.ibm.streamsx.watsonexplorer.test;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Parameter.param;
import static org.mockserver.model.ParameterBody.params;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.model.NottableString;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.watsonexplorer.operators.WEXPushOperator;
import com.ibm.streams.watsonexplorer.operators.WEXQueryOperator;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;
import com.ibm.streamsx.topology.spl.JavaPrimitive;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tester.Condition;

public class WatsonExplorerPushTest extends AbstractOperatorTest {
	
	private String wexPushResponseXMLStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		wexPushResponseXMLStr = new String(Files.readAllBytes(Paths.get("impl/test/resources/responses/wexpush_response.txt")));
	}
	
	@Override
	protected void initClient() {
	}

	@Test
	public void wexPushGreenthread() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("collection", "test"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "search-collection-enqueue"),
					param("crawl-urls", "<crawl-url status=\"complete\" url=\"ref://my_url\" synchronization=\"indexed\" enqueue-type=\"reenqueued\">\n" + 
							"  <crawl-data>\n" + 
							"    <text>test</text>\n" + 
							"  </crawl-data>\n" + 
							"</crawl-url>")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexPushResponseXMLStr));

		TStream<String> src = topo.strings("test");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring data, rstring url>"));
		
		opParams.put("username", "guest");
		opParams.put("password", "guest");
		opParams.put("collection", "test");
		
		JavaPrimitive.invokeJavaPrimitive(WEXPushOperator.class, splStream, OUTPUT_SCHEMA, opParams);		
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(com.ibm.streamsx.topology.context.StreamsContext.Type.EMBEDDED_TESTER); //.submit(topo, new HashMap<String, Object>()).get();
		topo.getTester().complete(context);
	}

	@Test
	public void wexPushDuplicatesProcessing() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("collection", "test"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "search-collection-enqueue"),
					param("crawl-urls", "<crawl-url status=\"complete\" url=\"ref://my_url\" synchronization=\"none\" enqueue-type=\"none\">\n" + 
							"  <crawl-data>\n" + 
							"    <text>test</text>\n" + 
							"  </crawl-data>\n" + 
							"</crawl-url>")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexPushResponseXMLStr));

		TStream<String> src = topo.strings("test");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring data, rstring url>"));
		
		opParams.put("username", "guest");
		opParams.put("password", "guest");
		opParams.put("collection", "test");
		opParams.put("duplicates", "skip");
		opParams.put("processing", "fast");		
		JavaPrimitive.invokeJavaPrimitive(WEXPushOperator.class, splStream, OUTPUT_SCHEMA, opParams);		
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(com.ibm.streamsx.topology.context.StreamsContext.Type.EMBEDDED_TESTER); //.submit(topo, new HashMap<String, Object>()).get();
		topo.getTester().complete(context);
	}	
	
	@Test
	public void wexPushAdditionalParams() throws Exception {
		client
		.when(request()
				.withMethod("POST")
				.withPath("/vivisimo/cgi-bin/velocity")
				.withHeader("Content-Type", "application/x-www-form-urlencoded; charset=ISO-8859-1")
				.withBody(params(
					param("collection", "test"),
					param("v.app", "api-rest"),
					param("v.username", "guest"),
					param("v.password", "guest"),
					param("v.function", "search-collection-enqueue"),
					param("crawl-urls", "<crawl-url status=\"complete\" url=\"ref://my_url\" synchronization=\"indexed\" enqueue-type=\"reenqueued\">\n" + 
							"  <crawl-data>\n" + 
							"    <text>test</text>\n" + 
							"  </crawl-data>\n" + 
							"</crawl-url>"),
					param("foo", "123"),
					param("bar", "987")
				))
		)
		.respond(response()
				.withStatusCode(200)
				.withBody(wexPushResponseXMLStr));

		TStream<String> src = topo.strings("test");
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPLQuery(), Type.Factory.getStreamSchema("tuple<rstring data, rstring url>"));
		
		opParams.put("username", "guest");
		opParams.put("password", "guest");
		opParams.put("collection", "test");
		opParams.put("additionalParams", new String[] {"foo=123", "bar=987"});
		JavaPrimitive.invokeJavaPrimitive(WEXPushOperator.class, splStream, OUTPUT_SCHEMA, opParams);		
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(com.ibm.streamsx.topology.context.StreamsContext.Type.EMBEDDED_TESTER); //.submit(topo, new HashMap<String, Object>()).get();
		topo.getTester().complete(context);
	}
	
	private static class StringStreamToSPLQuery implements BiFunction<String, OutputTuple, OutputTuple> {
		private static final long serialVersionUID = 1L;

		@Override
		public OutputTuple apply(String data, OutputTuple outTuple) {
			outTuple.setString("data", data);
			outTuple.setString("url", "my_url");
			return outTuple;
		}
	}
}
