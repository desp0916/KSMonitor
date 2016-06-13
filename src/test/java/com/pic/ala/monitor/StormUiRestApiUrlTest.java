package com.pic.ala.monitor;

import static com.pic.ala.monitor.StormUiRestApiUrl.trim;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StormUiRestApiUrlTest {

	private StormUiRestApiUrl stormUiUrl;
	private String scheme = StormUiRestApiUrl.SCHEME;
	private String host = StormUiRestApiUrl.HOST;
	private int port = StormUiRestApiUrl.PORT;
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
		String expected = String.format("%s://%s:%d/%s/configuration", scheme, host, port, trim(StormUiRestApiUrl.PATH_CLUSTER));
		String result = stormUiUrl.asClusterURL("configuration");
		assertEquals(expected, result);
	}

	@Test
	public void testSupervisorURL() {
		String expected = String.format("%s://%s:%d/%s/summary", scheme, host, port, trim(StormUiRestApiUrl.PATH_SUPERVISOR));
		String result = stormUiUrl.asSupervisorURL("summary");
		assertEquals(expected, result);
	}

	@Test
	public void testTopologyURL() {
		String expected = String.format("%s://%s:%s/%s/%s", scheme, host, port, trim(StormUiRestApiUrl.PATH_TOPOLOGY), topologyId);
		String result = stormUiUrl.withTopologyId(topologyId).asTopologyURL();
		System.out.println(expected);
		System.out.println(result);
		assertEquals(expected, result);
	}

	@Test
	public void testComponentURL() {
		String expected = String.format("%s://%s:%d/%s/%s/component/spout?", scheme, host, port, trim(StormUiRestApiUrl.PATH_TOPOLOGY), topologyId);
		String result = stormUiUrl.withTopologyId(topologyId).withComponent("spout").asComoponentURL();
		assertEquals(expected, result);
	}

	@Test
	public void testTopologoyOperationURL() {
		String expected = String.format("%s://%s:%d/%s/%s/kill/0", scheme, host, port, trim(StormUiRestApiUrl.PATH_TOPOLOGY), topologyId);
		String result = stormUiUrl.withTopologyId(topologyId).asTopologyURL("kill", "0");
		assertEquals(expected, result);
	}
}
