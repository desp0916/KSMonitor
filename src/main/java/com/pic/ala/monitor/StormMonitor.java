/**
 * http://www.opscoder.info/storm_monitor.html
 * http://www.opscoder.info/kafka_monitor.html
 * http://storm.apache.org/releases/0.10.0/STORM-UI-REST-API.html
 * http://hc.apache.org/httpcomponents-client-ga/quickstart.html
 * http://tutorials.jenkov.com/java-json/jackson-jsonparser.html
 */
package com.pic.ala.monitor;

import java.io.IOException;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class StormMonitor {


	public static void main(String[] args) {

		try {
			Content c = Request.Get("http://hdp02.localdomain:8744/api/v1/topology/ApLogAnalyzerV1-2-1465290576")
					.execute().returnContent();
			String topologyJson = c.asString();
			JsonFactory factory = new JsonFactory();
			JsonParser parser = factory.createParser(topologyJson);
			while (!parser.isClosed()) {
				JsonToken jsonToken = parser.nextToken();
//				System.out.println("jsonToken = " + jsonToken);

				if (JsonToken.FIELD_NAME.equals(jsonToken)) {
					String fieldName = parser.getCurrentName();
//					System.out.println(fieldName);

					jsonToken = parser.nextToken();
					System.out.println(parser.getCurrentName() + ": "+ parser.getValueAsString());
//
//					if ("status".equals(fieldName)) {
//						System.out.println(parser.getValueAsString());
//					} else if ("topologyStats".equals(fieldName)) {
//						System.out.println(parser.getValueAsString());
//					}
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

}
