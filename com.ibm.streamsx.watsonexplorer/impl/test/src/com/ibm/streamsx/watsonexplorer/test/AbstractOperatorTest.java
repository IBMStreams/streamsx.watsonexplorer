package com.ibm.streamsx.watsonexplorer.test;

import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.File;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;

import com.ibm.streams.operator.Operator;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.spl.JavaPrimitive;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tester.Condition;

public abstract class AbstractOperatorTest {

	public static final StreamSchema OUTPUT_SCHEMA = Type.Factory.getStreamSchema("tuple<rstring result>");
	protected static final String DEFAULT_HOST = "localhost";
	protected static final String COLLECTION_RESPONSE_PATH = "impl/test/resources/responses/collection_response_json.txt";
	protected static final String COLLECTION_NAME = "test-collection";
	protected static final String COLLECTION_ID = "col_12345";
	protected static final String COLLECTION_NAME2 = "test-collection2";
	protected static final String COLLECTION_ID2 = "col_98765";
	protected HashMap<String, Object> opParams;	
	
	static {
	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.setLevel(org.apache.log4j.Level.DEBUG);
	    
	    ConsoleAppender ca = new ConsoleAppender();
	    ca.setWriter(new OutputStreamWriter(System.out));
	    ca.setLayout(new PatternLayout("%-5p [%t]: %m%n"));
	    rootLogger.addAppender(ca);
	}
	
	@Rule
	public MockServerRule mockServerRule = new MockServerRule(this);
	protected MockServerClient client;
	
	protected String collectionResponseStr;
	protected Topology topo;
	
	@Before
	public void setup() throws Exception {
		
		//Topology.STREAMS_LOGGER.setLevel(Level.ALL);
		Topology.TOPOLOGY_LOGGER.setLevel(Level.ALL);
		collectionResponseStr = new String(Files.readAllBytes(Paths.get(COLLECTION_RESPONSE_PATH)));
		
		// create topology
		topo = new Topology("test");
		SPL.addToolkit(topo, new File("../com.ibm.streamsx.watsonexplorer"));
		
		opParams = new HashMap<String, Object>();
		opParams.put("host", "localhost");
		opParams.put("port", getPort());
		
		client = new MockServerClient(getHost(), getPort());
		initClient();
	}

	protected void initClient() {
		// operator will always query for the collection ID
		client
			.when(request()
					.withMethod("POST")
					.withPath("/api/v10/collections"))
			.respond(response()
					.withStatusCode(200)
					.withBody(collectionResponseStr));
	}
	
	protected int getPort() {
		return mockServerRule.getPort();
	}
	
	protected String getHost() {
		return DEFAULT_HOST;
	}
	
	protected void runTopology(SPLStream inputStream, String expectedResult, Class<? extends Operator> opClazz) throws Exception {
		runTopology(inputStream, new String[]{expectedResult}, opClazz);
	}
	
	protected void runTopology(SPLStream inputStream, String[] expectedResults, Class<? extends Operator> opClazz) throws Exception {
		SPLStream splResultStream = JavaPrimitive.invokeJavaPrimitive(opClazz, inputStream, OUTPUT_SCHEMA, opParams);
		TStream<String> resultStream = SPLStreams.toStringStream(splResultStream);
		
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(com.ibm.streamsx.topology.context.StreamsContext.Type.EMBEDDED_TESTER); //.submit(topo, new HashMap<String, Object>()).get();
		Condition<List<String>> contents = topo.getTester().completeAndTestStringOutput(context, resultStream, 5, TimeUnit.SECONDS, expectedResults);
		
		Assert.assertTrue(contents.getResult().size() > 0);
		assertTrue(contents.getResult().toString(), contents.valid());
	}
	
	protected void runTopologyNoOutputPort(SPLStream inputStream, Class<? extends Operator> opClazz) throws Exception {
		JavaPrimitive.invokeJavaPrimitiveSink(opClazz, inputStream, opParams);
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(com.ibm.streamsx.topology.context.StreamsContext.Type.EMBEDDED);
		
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(ContextProperties.TRACING_LEVEL, TraceLevel.TRACE);
		context.submit(topo, config).get();
	}
}

