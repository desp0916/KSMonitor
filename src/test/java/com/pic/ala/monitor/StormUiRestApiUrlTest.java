package com.pic.ala.monitor;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StormUiRestApiUrlTest {

	private StormUiRestApiUrl stormUiUrl;
	private String scheme = StormUiRestApiUrl.SCHEME;
	private String host = StormUiRestApiUrl.HOST;
	private String port = StormUiRestApiUrl.PORT;
	private String topologyId = "LogAnalyzerV1-1-1465290461";

	@Before
	public void setUp() {
		stormUiUrl = new StormUiRestApiUrl().withHost(host);
	}

	@After
	public void tearDown() {
		stormUiUrl = null;
	}

	@Test
	public void testClusterURL() {
		String expected = scheme + "://" + host + ":" + port + StormUiRestApiUrl.PATH_CLUSTER + "configuration";
		String result = stormUiUrl.asClusterURL("configuration");
		assertEquals(expected, result);
	}

	@Test
	public void testSupervisorURL() {
		String expected = "http://" + host + ":" + port + StormUiRestApiUrl.PATH_SUPERVISOR + "summary";
		String result = stormUiUrl.asSupervisorURL("summary");
		assertEquals(expected, result);
	}

	@Test
	public void testTopologyURL() {
		String expected = scheme + "://" + host + ":" + port + StormUiRestApiUrl.PATH_TOPOLOGY + topologyId + "?";
		String result = stormUiUrl.withTopologyId(topologyId).asTopologyURL();
		assertEquals(expected, result);
	}

	@Test
	public void testComponentURL() {
		String expected = scheme + "://" + host + ":" + port + StormUiRestApiUrl.PATH_TOPOLOGY + topologyId
				+ "/component/spout?";
		String result = stormUiUrl.withTopologyId(topologyId).withComponent("spout").asComoponentURL();
		assertEquals(expected, result);
	}

	@Test
	public void testTopologoyOperationURL() {
		String expected = scheme + "://" + host + ":" + port + StormUiRestApiUrl.PATH_TOPOLOGY + topologyId + "/kill/0";
		String result = stormUiUrl.withTopologyId(topologyId).asTopologyURL("kill", "0");
		assertEquals(expected, result);
	}
}
