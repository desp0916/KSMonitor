/**
 * http://www.opscoder.info/storm_monitor.html
 * http://www.opscoder.info/kafka_monitor.html
 * http://storm.apache.org/releases/0.10.0/STORM-UI-REST-API.html
 * http://hc.apache.org/httpcomponents-client-ga/quickstart.html
 * http://tutorials.jenkov.com/java-json/jackson-jsonparser.html
 */
package com.pic.ala.monitor;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class StormMonitor {

	private String scheme;
	private String host;
	private int port;
	private String path;
	private String topologyId;
	private String queryString;
	private String component;


	StormMonitor() {
	}

	public StormMonitor(String host) {
		this.host = host;
	}

	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getTopologyId() {
		return topologyId;
	}

	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

	public static void main(String[] args) {

		String host = "hdp02.localdomain";
		int port = 8744;
		String topologyId = "ApLogGeneratorV1-2-1465033997";

		StormMonitor stormMonitor = new StormMonitor(host);
		stormMonitor.setPort(port);

		String clusterJson = stormMonitor.getClusterJson();
		System.out.println(clusterJson);

		String supervisorJson = stormMonitor.getSupervisorJson();
		System.out.println(supervisorJson);

		stormMonitor.setTopologyId(topologyId);
		String topologyJson = stormMonitor.getTopologyJson();
		System.out.println(topologyJson);



	}

	public static void mainx(String[] args) {
		try {
			String clusterUrl = new StormUiRestApiUrl().withHost("hdp02.localdomain").asClusterURL("configuration");
			String topologyUrl = new StormUiRestApiUrl().withHost("hdp02.localdomain").withPort(8744)
					.withTopologyId("ApLogGeneratorV1-2-1465033997").asTopologyURL();
			System.out.println(topologyUrl);
			Content c = Request.Get(topologyUrl).execute().returnContent();
			String topologyJson = c.asString();
			JsonFactory factory = new JsonFactory();
			JsonParser parser = factory.createParser(topologyJson);
			while (!parser.isClosed()) {
				JsonToken jsonToken = parser.nextToken();
				// System.out.println("jsonToken = " + jsonToken);

				if (JsonToken.FIELD_NAME.equals(jsonToken)) {
					String fieldName = parser.getCurrentName();
					// System.out.println(fieldName);

					jsonToken = parser.nextToken();
					System.out.println(parser.getCurrentName() + ": " + parser.getValueAsString());
					//
					// if ("status".equals(fieldName)) {
					// System.out.println(parser.getValueAsString());
					// } else if ("topologyStats".equals(fieldName)) {
					// System.out.println(parser.getValueAsString());
					// }
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// try {
		// Content c = Request.Post("http://targethost/login")
		// .bodyForm(Form.form().add("username", "vip").add("password",
		// "secret").build()).execute()
		// .returnContent();
		// System.out.println(c.asString());
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public String getClusterJson() {
		String url = new StormUiRestApiUrl().withHost(host).withPort(port).asClusterURL();
		return this.getJson(url);
	}

	public String getSupervisorJson() {
		return getSupervisorJson("");
	}

	public String getSupervisorJson(String operation) {
		String url;
		if (operation == null || operation.length() == 0) {
			url = new StormUiRestApiUrl().withHost(host).withPort(port).asSupervisorURL();
		} else {
			url = new StormUiRestApiUrl().withHost(host).withPort(port).asSupervisorURL(operation);
		}
		return this.getJson(url);
	}

	public String getTopologyJson() {
		String url = new StormUiRestApiUrl().withHost(host).withPort(port)
				.withTopologyId(topologyId).asTopologyURL();
		return this.getJson(url);
	}

	public String getJson(String url) {
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
}
