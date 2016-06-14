/**
 * http://www.opscoder.info/storm_monitor.html
 * http://www.opscoder.info/kafka_monitor.html
 * http://storm.apache.org/releases/0.10.0/STORM-UI-REST-API.html
 * http://hc.apache.org/httpcomponents-client-ga/quickstart.html
 * http://tutorials.jenkov.com/java-json/jackson-jsonparser.html
 */
package com.pic.ala.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StormMonitor {

	private String scheme = StormUiRestApiUrl.SCHEME;
	private StormUiRestApiUrl url = new StormUiRestApiUrl();
	private Map<String, Object> config = new HashMap<String, Object>();

	private void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public StormMonitor() {
		this.setUrl(StormUiRestApiUrl.HOST, StormUiRestApiUrl.PORT, null, null, null);
	}

	public StormMonitor(String host) {
		this.setUrl(host, StormUiRestApiUrl.PORT, null, null, null);
	}

	public StormMonitor(String host, int port) {
		this.setUrl(host, port, null, null, null);
	}

	public void setUrl(String host, int port, String topologyId, String component, String queryString) {
		this.url.withScheme(scheme).withHost(host).withPort(port).withTopologyId(topologyId).withComponent(component);
	}

	public String getClusterJson() {
		return getClusterJson("");
	}

	public String getClusterJson(String operation) {
		return getJsonResponse(this.url.withScheme(scheme).asClusterURL(operation));
	}

	public String getSupervisorJson() {
		return getSupervisorJson("");
	}

	public String getSupervisorJson(String operation) {
		return getJsonResponse(this.url.withScheme(scheme).asSupervisorURL(operation));
	}

	public String getTopologyJson() {
		return getJsonResponse(this.url.withScheme(scheme).asTopologyURL());
	}

	public String getTopologyJson(String topologyId) {
		return getJsonResponse(this.url.withScheme(scheme).withTopologyId(topologyId).asTopologyURL());
	}

	public String getTopologyJson(String topologyId, String operation, int waitTime) {
		return getJsonResponse(this.url.withScheme(scheme).withTopologyId(topologyId).asTopologyURL(operation, waitTime));
	}

	public String getComponentJson(String topologyId, String component) {
		return getJsonResponse(this.url.withScheme(scheme).withTopologyId(topologyId).withComponent(component).asComoponentURL());
	}

	public static String getJsonResponse(String url) {
		System.out.println(url);
		Content c = null;
		String json = "";
		try {
			c = Request.Get(url).execute().returnContent();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (c != null) {
			json = c.asString();
		}
		return json;
	}

	public static void main(String[] args) {
		StormMonitor sm = new StormMonitor("hdp02.localdomain", 8744);
//		sm.setScheme("https");

//		String clusterJson = sm.getClusterJson();
//		System.out.println("clusterJson:" + clusterJson);
//
//		String supervisorJson = sm.getSupervisorJson();
//		System.out.println("supervisorJson:" + supervisorJson);
//
//		String topologySummaryJson = sm.getTopologyJson();
//		System.out.println("topologySummaryJson:" + topologySummaryJson);
//
//		String topologyJson = sm.getTopologyJson("ApLogAnalyzerV1-2-1465290576");
//		System.out.println("topologyJson:" + topologyJson);
//
//		String componentJson = sm.getComponentJson("ApLogAnalyzerV1-2-1465290576", "kafkaSpout");
//		System.out.println("componentJson: " + componentJson);

		List<JsonNode> allTopologies = getAllTopologies();

		for (JsonNode topology : allTopologies) {
			String topologyId = topology.get("id").asText();
			String topologyJson = sm.getTopologyJson(topologyId);
			System.out.println(topologyJson);
		}
	}

	private static List<JsonNode> getAllTopologies() {
		List<JsonNode> allTopologies = new ArrayList<JsonNode>();
		ObjectMapper objectMapper = new ObjectMapper();

		try {
			StormMonitor sm = new StormMonitor("hdp02.localdomain", 8744);
			String topologySummaryJson = sm.getTopologyJson();
			// http://tutorials.jenkov.com/java-json/jackson-objectmapper.html
			JsonNode node = objectMapper.readValue(topologySummaryJson, JsonNode.class);
		    JsonNode topologies = node.get("topologies");
		    for (int i = 0; i < topologies.size(); i++) {
			    JsonNode topology = topologies.get(i);
			    allTopologies.add(topology);
		    }
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return allTopologies;
	}

}