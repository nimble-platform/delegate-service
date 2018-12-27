package eu.nimble.service.delegate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.nimble.service.delegate.EurekaEntry;
import eu.nimble.service.delegate.EurekaEntry.Status;

public class EurekaClient {
	
		//static String EurekaURL ="http://localhost";
	static String EurekaURL ="http://9.148.58.39";
		
		String hostName;
		String appName;
		String ipAddr;
		
		public EurekaClient(String hostName, String appName, String ipAddr) {
			this.hostName = hostName;
			this.appName = appName;
			this.ipAddr = ipAddr;
		}
		
		
		public ArrayList<EurekaEntry> discover() {
			try {
				String fullURL = EurekaURL + ":8761/eureka/apps";
				HttpURLConnection conn = (HttpURLConnection) ( new URL(fullURL)).openConnection();
				conn.setRequestMethod("GET");
				ObjectMapper mapper1 = new ObjectMapper();
				conn.setRequestProperty("Content-Type", "application/json");
				conn.setRequestProperty("accept", "application/json");
				conn.setDoOutput(true);
				conn.setDoInput(true);
				
				BufferedReader reader;
				
				if (200 <= conn.getResponseCode() && conn.getResponseCode() <= 299) {
					reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
					StringBuilder builder = new StringBuilder();
					String output;
					while ((output = reader.readLine()) != null) {
						builder.append(output);
					}
					JsonNode dataObj = mapper1.readTree(builder.toString());
					java.util.List<JsonNode> instancesList = dataObj.findValues("instance");
					int numReturned = instancesList.size();
					ArrayList<EurekaEntry> entries = new ArrayList<EurekaEntry>();
					for (int i=0; i<numReturned; i++) {
						entries.add(new EurekaEntry(instancesList.get(i).findValue("app").textValue(), instancesList.get(i).findValue("ipAddr").textValue(), Status.OUT_OF_SERVICE));
					}
					return entries;
					
						
				} else {
					reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
					for (String line; (line = reader.readLine()) != null;) {
				        System.out.println(line);
					}
				}
				
				reader.close();
			} catch (ProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException jsonException) {
				// TODO: what todo?
				jsonException.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return null;
		}
	
		
		public void unRegister() {
			try {
				String fullURL = EurekaURL + ":8761/eureka/apps/" + appName + "/" + hostName;
				HttpURLConnection conn = (HttpURLConnection) ( new URL(fullURL)).openConnection();
				conn.setRequestMethod("DELETE");
				conn.setRequestProperty("Content-Type", "application/json");
				conn.setRequestProperty("accept", "application/json");
				conn.setDoOutput(true);
				conn.setDoInput(true);	
				
				int x=conn.getResponseCode();
					
				
			} catch (ProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException jsonException) {
				// TODO: what todo?
				jsonException.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
	public String register() {
		
		try {
			String fullURL = EurekaURL + ":8761/eureka/apps/" + appName;
			HttpURLConnection conn = (HttpURLConnection) ( new URL(fullURL)).openConnection();
			conn.setRequestMethod("POST");
			ObjectMapper mapper = new ObjectMapper();
			HttpPostData httpPostData = new HttpPostData(hostName, appName, ipAddr);
			String data = "{\"instance\": " + mapper.writeValueAsString(httpPostData)+"}";
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("accept", "application/json");
			conn.setDoOutput(true);
			conn.setDoInput(true);	 
			try (OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream())) {
				out.write(data);
				out.close();
				BufferedReader reader;
				
				if (200 <= conn.getResponseCode() && conn.getResponseCode() <= 299) {
					reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				} else {
					reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
				}
				
				for (String line; (line = reader.readLine()) != null;) {
			        System.out.println(line);
				}
				
				reader.close();
			}
		} catch (ProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException jsonException) {
			// TODO: what todo?
			jsonException.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return "1";
	}
	
}

