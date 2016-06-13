/**
 * Storm UI REST API URL Generator
 *
 * You can use this class to generate the URL for Storm UI REST API.
 *
 * For example:
 * <code>
 *    StormUiRestApiUrl sus = new StormUiRestApiUrl().withHost("hdp02.localdomain")
 *        .withTopologyId("LogAnalyzerV1-1-1465290461").withComponent("spout");
 *
 *    String urlCluster = sus.asClusterURL("configuration");
 *    System.out.println(urlCluster);
 *
 *    String urlSupervisor = sus.asSupervisorURL("summary");
 *    System.out.println(urlSupervisor);
 *
 *    String urlTopology = sus.asTopologyURL();
 *    System.out.println(urlTopology);
 *
 *    String urlComponent = sus.asComoponentURL();
 *    System.out.println(urlComponent);
 *
 *    String urlTopology2 = sus.asTopologyURL("activate", "0");
 *    System.out.println(urlTopology2);
 * </code>
 *
 * This class is based on Storm 0.10.0, so please refer to the document:
 * http://storm.apache.org/releases/0.10.0/STORM-UI-REST-API.html
 *
 * This class Currently supports the following operations:
 *
 * /api/v1/cluster/configuration (GET)
 * /api/v1/cluster/summary (GET)
 * /api/v1/supervisor/summary (GET)
 * /api/v1/topology/summary (GET)
 * /api/v1/topology/:id (GET)
 * /api/v1/topology/:id/component/:component (GET)
 * /api/v1/topology/:id/activate (POST)
 * /api/v1/topology/:id/deactivate (POST)
 * /api/v1/topology/:id/rebalance/:wait-time (POST)
 * /api/v1/topology/:id/kill/:wait-time (POST)
 */
package com.pic.ala.monitor;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class StormUiRestApiUrl {

	private String scheme;
	private String host;
	private int port;
	private String path;
	private String topologyId;
	private String queryString;
	private String component;
	public static final String SCHEME = "http";
	public static final String HOST = "localhost";
	public static final int PORT = 8080;
	public static final String PATH_CLUSTER = "/api/v1/cluster/";
	public static final String PATH_SUPERVISOR = "/api/v1/supervisor/";
	public static final String PATH_TOPOLOGY = "/api/v1/topology/";

	public StormUiRestApiUrl() {
		this.scheme = SCHEME;
		this.host = HOST;
		this.port = PORT;
		this.path = PATH_TOPOLOGY;
		this.topologyId = "";
		this.queryString = "";
		this.component = "";
	}

	public String getScheme() {
		return scheme;
	}

	public StormUiRestApiUrl withScheme(String scheme) {
		this.scheme = scheme;
		return this;
	}

	public String getHost() {
		return host;
	}

	public StormUiRestApiUrl withHost(String host) {
		this.host = host;
		return this;
	}

	public int getPort() {
		return port;
	}

	public StormUiRestApiUrl withPort(int port) {
		this.port = port;
		return this;
	}

	public String getPath() {
		return path;
	}

	public StormUiRestApiUrl withPath(String path) {
		this.path = trim(path);
		return this;
	}

	public String getComponent() {
		return component;
	}

	public StormUiRestApiUrl withComponent(String component) {
		this.component = component;
		return this;
	}

	public String getTopologyId() {
		return topologyId;
	}

	public StormUiRestApiUrl withTopologyId(String topologyId) {
		this.topologyId = topologyId;
		return this;
	}

	public String getQueryString() {
		return queryString;
	}

	public StormUiRestApiUrl withQueryString(String queryString) {
		this.queryString = queryString;
		return this;
	}

	public String asClusterURL() {
		return this.asClusterURL("");
	}
	/**
	 *
	 * /api/v1/cluster/configuration (GET)
	 * /api/v1/cluster/summary (GET)
	 *
	 * @param operation		可以是 configuration 或 summary
	 * @return
	 */
	public String asClusterURL(String operation) {
		this.withPath(PATH_CLUSTER);
		String[] validOperations = new String[] {"configuration", "summary"};
		if (operation == null || !Arrays.asList(validOperations).contains(operation)) {
			operation = "summary";
		}
		String URL = String.format("%s://%s:%d/%s/%s", scheme, host, port, path, operation);
		return URL;
	}

	public String asSupervisorURL() {
		return this.asSupervisorURL("");
	}
	/**
	 *
	 * /api/v1/supervisor/summary (GET)
	 *
	 * @param operation		可以是 configuration 或 summary
	 * @return
	 */
	public String asSupervisorURL(String operation) {
		this.withPath(PATH_SUPERVISOR);
		String[] validOperations = new String[] {"summary"};
		if (operation == null || !Arrays.asList(validOperations).contains(operation)) {
			operation = "summary";
		}
		String URL = String.format("%s://%s:%d/%s/%s", scheme, host, port, path, operation);
		return URL;
	}

	/**
	 * /api/v1/topology/:id (GET)
	 *
	 * @return
	 */
	public String asTopologyURL() {
		return this.asTopologyURL("", "");
	}

	/**
	 * /api/v1/topology/:id/activate (POST)
	 * /api/v1/topology/:id/deactivate (POST)
	 * /api/v1/topology/:id/rebalance/:wait-time (POST)
	 * /api/v1/topology/:id/kill/:wait-time (POST)
	 */
	public String asTopologyURL(String operation, String waitTime) {
		this.withPath(PATH_TOPOLOGY);
		String URL = String.format("%s://%s:%s/%s/%s", scheme, host, port, path, topologyId);
		String[] validOperations = new String[] {"activate", "deactivate", "rebalance", "kill"};
		if ((operation == null || operation.length() == 0) && (waitTime == null || waitTime.length() == 0)) {
			URL = String.format("%s://%s:%d/%s/%s", scheme, host, port, path, topologyId);
			if (queryString != null && queryString.length() > 0) {
				URL = String.format("%s?%s", URL, queryString);
			}
		} else if (operation == null || !Arrays.asList(validOperations).contains(operation)) {
			throw new InvalidParameterException("Invalid operation parameter: " + operation.toString());
		} else if (("rebalance").equals(operation) || ("kill").equals(operation)) {

			if (waitTime == null || Integer.parseInt(waitTime) < 0) {
				waitTime = "0";
			}
			URL = String.format("%s/%s/%s", URL, operation, waitTime);
		} else {
			URL = String.format("%s/%s", URL, operation);
		}
		return URL;
	}

	/**
	 * /api/v1/topology/:id/component/:component (GET)
	 *
	 * @return
	 */
	public String asComoponentURL() {
		String URL = String.format("%s/component/%s?%s", this.asTopologyURL(), component, queryString);
		return URL;
	}

	/**
	 * 移除字串結尾的 "/" 字元
	 *
	 * @param s
	 * @return
	 */
	public static String ltrim(String s) {
		Pattern LTRIM = Pattern.compile("^\\/+");
		return LTRIM.matcher(s).replaceAll("");
	}

	/**
	 * 移除字串開頭的 "/" 字元
	 *
	 * @param s
	 * @return
	 */
	public static String rtrim(String s) {
		Pattern RTRIM = Pattern.compile("\\/+$");
		return RTRIM.matcher(s).replaceAll("");
	}

	public static String trim(String s) {
		return rtrim(ltrim(s));
	}
	public static void main(String args[]) {

	}

}