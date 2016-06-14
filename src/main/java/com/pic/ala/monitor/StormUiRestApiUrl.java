/**
 * Storm UI REST API URL Generator
 *
 * You could use this class to generate the URLs for Storm UI REST API.
 *
 * For examples:
 *
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
 * This class is based on Storm 0.10.0, so please refer to the document at first:
 * http://storm.apache.org/releases/0.10.0/STORM-UI-REST-API.html
 *
 * This class currently supports the following operations:
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

import java.util.Arrays;
import java.util.regex.Pattern;

public class StormUiRestApiUrl {

	private String scheme;		// http or https
	private String host;		// hostname or IP address
	private int port;			// the port which Storm UI is listening
	private String path;
	private String topologyId;
	private String queryString;
	private String component;
	private static int WAIT_TIME = 30;	// seconds
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
		this.path = trimSlashes(path);
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

	/**
	 * Default operation is "summary", so the URL string
	 * "/api/v1/cluster/summary" will be returned.
	 *
	 * @return		Storm REST API URL
	 */
	public String asClusterURL() {
		return this.asClusterURL("");
	}

	/**
	 * One of the following URL strings will be returned:
	 *
	 * <ol>
	 * <li>/api/v1/cluster/configuration (GET)</li>
	 * <li>/api/v1/cluster/summary (GET)</li>
	 * </ol>
	 *
	 * @param operation		可以是 configuration 或 summary(default)
	 * @return				Storm REST API URL
	 */
	public String asClusterURL(String operation) {
		this.withPath(PATH_CLUSTER);
		String[] validOperations = new String[] {"configuration", "summary"};
		if (operation == null || operation.length() == 0) {
			operation = "summary";
		} else if (!Arrays.asList(validOperations).contains(operation)) {
			throw new IllegalArgumentException("Invalid operation parameter: " + operation.toString());
		}
		String URL = String.format("%s://%s:%d/%s/%s", scheme, host, port, path, operation);
		return URL;
	}

	/**
	 * Default operation is "summary", so the URL string
	 * "/api/v1/supervisor/summary" will be returned.
	 *
	 * @return		Storm REST API URL
	 */
	public String asSupervisorURL() {
		return this.asSupervisorURL("");
	}

	/**
	 * The following URLs patterns will be returned:
	 *
	 * <ol>
	 * <li>/api/v1/supervisor/summary (GET)</li>
	 * </ol>
	 *
	 * @param operation		只可以是 summary
	 * @return				Storm REST API URL
	 */
	public String asSupervisorURL(String operation) {
		this.withPath(PATH_SUPERVISOR);
		String[] validOperations = new String[] {"summary"};
		if (operation == null || operation.length() == 0) {
			operation = "summary";
		} else if (!Arrays.asList(validOperations).contains(operation)) {
			throw new IllegalArgumentException("Invalid operation parameter: " + operation.toString());
		}
		String URL = String.format("%s://%s:%d/%s/%s", scheme, host, port, path, operation);
		return URL;
	}

	/**
	 * Returns topology information and statistics. Set the topology id using withTopologyId().
	 * If topology id is not set, so the URL string "/api/v1/topology/summary" which
	 * provides summary information for all topologies will be returned.
	 *
	 * @return		Storm REST API URL
	 */
	public String asTopologyURL() {
		return this.asTopologyURL("", WAIT_TIME);
	}

	/**
	 * Returns topology information and statistics. Set the topology id using withTopologyId().
	 * If topology id is not set, the URL "/api/v1/topology/summary" which provides summary
	 * information for all topologies will be returned.
	 *
	 * One of the following URL patterns will be returned:
	 *
	 * <ul>
	 * <li>/api/v1/topology/summary (GET)</li>
	 * <li>/api/v1/topology/:id/activate (POST)</li>
	 * <li>/api/v1/topology/:id/deactivate (POST)</li>
	 * <li>/api/v1/topology/:id/rebalance/:wait-time (POST)</li>
	 * <li>/api/v1/topology/:id/kill/:wait-time (POST)</li>
	 * </ul>
	 *
	 * @param operation		To activate/dectivate/rebalance/kill some topology
	 * @param waitTime		Wait time before the operation happens
	 * @return				Storm REST API URL
	 */
	public String asTopologyURL(String operation, int waitTime) {
		this.withPath(PATH_TOPOLOGY);
		String URL;
		// Returns summary information for all topologies.
		if (this.topologyId == null || this.topologyId.length() == 0) {
			URL = String.format("%s://%s:%s/%s/summary", scheme, host, port, path);
		} else {
			URL = String.format("%s://%s:%s/%s/%s", scheme, host, port, path, topologyId);
			String[] validOperations = new String[] {"activate", "deactivate", "rebalance", "kill"};
			if (operation == null || operation.length() == 0) {
				URL = String.format("%s://%s:%d/%s/%s", scheme, host, port, path, topologyId);
				if (queryString != null && queryString.length() > 0) {
					URL = String.format("%s?%s", URL, queryString);
				}
			} else if (!Arrays.asList(validOperations).contains(operation)) {
				throw new IllegalArgumentException("Invalid operation parameter: " + operation.toString());
			} else if (("rebalance").equals(operation) || ("kill").equals(operation)) {
				if (waitTime < 0) {
					waitTime = WAIT_TIME;
				}
				URL = String.format("%s/%s/%d", URL, operation, waitTime);
			} else {
				URL = String.format("%s/%s", URL, operation);
			}
		}
		return URL;
	}

	/**
	 * Returns detailed metrics and executor information.
	 *
	 * The following URL pattern will be returned:
	 * <ul>
	 * <li>/api/v1/topology/:id/component/:component (GET)</li>
	 * </ul>
	 *
	 * @return		Storm REST API URL
	 */
	public String asComoponentURL() {
		String URL = String.format("%s/component/%s", this.asTopologyURL(), component);
		if (queryString != null && queryString.length() > 0) {
			URL = String.format("%s?%s", URL, queryString);
		}
		return URL;
	}

	/**
	 * Trims slash "/" at the beginning of the string.
	 *
	 * @param s		String to trim
	 * @return		String without "/" at the beginning
	 */
	public static String ltrimBackslash(String s) {
		Pattern LTRIM = Pattern.compile("^\\/+");
		return LTRIM.matcher(s).replaceAll("");
	}

	/**
	 * Trims slash "/" at the end of the string.
	 *
	 * @param s		String to trim
	 * @return		String without "/" at the end
	 */
	public static String rtrimBackslash(String s) {
		Pattern RTRIM = Pattern.compile("\\/+$");
		return RTRIM.matcher(s).replaceAll("");
	}

	/**
	 * Trims slashes "/" at the beginning and end of the string.
	 *
	 * @param s		String to trim
	 * @return		String without "/" at the beginning
	 */
	public static String trimSlashes(String s) {
		return rtrimBackslash(ltrimBackslash(s));
	}

}