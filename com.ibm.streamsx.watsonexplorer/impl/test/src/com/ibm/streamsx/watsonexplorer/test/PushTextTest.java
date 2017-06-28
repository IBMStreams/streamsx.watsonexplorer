package com.ibm.streamsx.watsonexplorer.test;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Parameter.param;
import static org.mockserver.model.ParameterBody.params;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.watsonexplorer.operators.CAPushText;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

public class PushTextTest extends AbstractOperatorTest {

	private static final String INPUT_TEXT = "Testing";
	private static final String DOC_ID = "mydoc.txt";
	private static final String FIRST_NAME_TEXT = "John";
	private static final StreamSchema INPUT_SCHEMA = Type.Factory.getStreamSchema("tuple<rstring docId, rstring text, rstring firstname>");
	private String pushTextResponseStr;
	private String loginResponseStr;
	private String listFieldsResponseStr;
	
	@Before
	public void setup() throws Exception {
		super.setup();
		collectionResponseStr = new String(Files.readAllBytes(Paths.get("impl/test/resources/responses/collection_response_json2.txt")));
		pushTextResponseStr = new String(Files.readAllBytes(Paths.get("impl/test/resources/responses/pushtext_response.txt")));
		loginResponseStr = new String(Files.readAllBytes(Paths.get("impl/test/resources/responses/login_response.txt")));
		listFieldsResponseStr = new String(Files.readAllBytes(Paths.get("impl/test/resources/responses/listfields_response.txt")));
	}
	
	@Override
	protected void initClient() {
	}
	
	@Test
	public void pushTextGreenThread() throws Exception {
		HttpRequest loginRequest = request()
				.withMethod("POST")
				.withPath("/api/v20/admin/login")
				.withBody(params(
						param("output", "application/json"),
						param("username", "esadmin"),
						param("password", "test")
				));
		
		HttpRequest collectionsListRequest = request()
				.withMethod("POST")
				.withPath("/api/v20/admin/collections/list")
				.withBody(params(
						param("output", "application/json"),
						param("securityToken", "my-security-token")
				));

		HttpRequest addTextRequest = request()
				.withMethod("POST")
				.withPath("/api/v20/admin/collections/indexer/document/add/text")
				.withBody("collection=col_98765&securityToken=my-security-token&documentId=mydoc.txt&text=Testing&fields=%7B%22firstname%22%3A%22John%22%7D");
		
		HttpRequest listFieldsRequest = request()
				.withMethod("POST")
				.withPath("/api/v20/admin/collections/indexer/fields/list")
				.withBody(params(
						param("output", "application/json"),
						param("securityToken", "my-security-token"),
						param("collection", COLLECTION_ID2)
				));
		
		HttpRequest logoutRequest = request()
				.withMethod("POST")
				.withPath("/api/v20/admin/logout")
				.withBody(params(param("securityToken", "my-security-token")));
		
		client
		.when(loginRequest)
		.respond(response()
				.withStatusCode(200)
				.withBody(loginResponseStr));
		
		client
		.when(collectionsListRequest)
		.respond(response()
				.withStatusCode(200)
				.withBody(collectionResponseStr));
		
		client.when(listFieldsRequest)
		.respond(response()
				.withStatusCode(200)
				.withBody(listFieldsResponseStr));
		
		client.when(addTextRequest)
		.respond(response()
				.withStatusCode(200)
				.withBody(pushTextResponseStr));		
		
		client.when(logoutRequest)
		.respond(response()
				.withStatusCode(200));
		
		TStream<String> src = topo.strings(INPUT_TEXT);
		SPLStream splStream = SPLStreams.convertStream(src, new StringStreamToSPL(), INPUT_SCHEMA);
		
		opParams.put("collectionName", COLLECTION_NAME2);
		opParams.put("documentIdAttr", INPUT_SCHEMA.getAttribute("docId"));
		opParams.put("username", "esadmin");
		opParams.put("password", "test");
		opParams.put("fieldAttributeNames", new String[]{"firstname"});
		
		runTopologyNoOutputPort(splStream, CAPushText.class);
		
		// test verifications
		client.verify(loginRequest, VerificationTimes.atLeast(2));
		client.verify(collectionsListRequest, VerificationTimes.once());
		client.verify(listFieldsRequest, VerificationTimes.once());
		client.verify(addTextRequest, VerificationTimes.once());
		client.verify(logoutRequest, VerificationTimes.once());
		
	}
	
	private static class StringStreamToSPL implements BiFunction<String, OutputTuple, OutputTuple> {
		private static final long serialVersionUID = 1L;

		@Override
		public OutputTuple apply(String text, OutputTuple outTuple) {
			outTuple.setString("docId", DOC_ID);
			outTuple.setString("text", text);
			outTuple.setString("firstname", FIRST_NAME_TEXT);
			return outTuple;
		}
	}
}
