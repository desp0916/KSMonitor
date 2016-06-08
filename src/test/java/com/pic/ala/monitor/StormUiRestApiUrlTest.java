package com.pic.ala.monitor;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StormUiRestApiUrlTest {

	private StormUiRestApiUrl stormUiUrl;

	@Before
	public void setUp() {
		stormUiUrl = new StormUiRestApiUrl()
				.withHost("hdp02.localdomain")
				.withTopologyId("LogAnalyzerV1-1-1465290461")
				.withComponent("spout");
	}

	@After
	public void tearDown() {
		stormUiUrl = null;
	}

	@Test
	public void testClusterURL() {
		String expected = "http://hdp02.localdomain:8744/api/v1/cluster/configuration";
		String result = stormUiUrl.asClusterURL("configuration");
		assertEquals(expected, result);
	}

	@Test
	public void testSupervisorURL() {
		String expected = "http://hdp02.localdomain:8744/api/v1/supervisor/summary";
		String result = stormUiUrl.asSupervisorURL("summary");
		assertEquals(expected, result);
	}

	@Test
	public void testTopologyURL() {
		String expected = "http://hdp02.localdomain:8744/api/v1/topology/LogAnalyzerV1-1-1465290461?";
		String result = stormUiUrl.asTopologyURL();
		assertEquals(expected, result);
	}

	@Test
	public void testComponentURL() {
		String expected = "http://hdp02.localdomain:8744/api/v1/topology/LogAnalyzerV1-1-1465290461/component/spout?";
		String result = stormUiUrl.asComoponentURL();
		assertEquals(expected, result);
	}

	@Test
	public void testTopologoyOperationURL() {
		String expected = "http://hdp02.localdomain:8744/api/v1/topology/LogAnalyzerV1-1-1465290461/kill/0";
		String result = stormUiUrl.asTopologyURL("kill", "0");
		assertEquals(expected, result);
	}
}
