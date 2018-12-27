package eu.nimble.service.delegate;

import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;

public class TestEurekaClient {
	
		public static void main(String[] args) throws JsonProcessingException {

			EurekaClient client1 = new EurekaClient("localhost","app1","10.10.10.10");
			EurekaClient client2 = new EurekaClient("localhost","app2","10.10.10.10");
			EurekaClient client3 = new EurekaClient("localhost","app3","10.10.10.10");
			
			client1.register();
			client2.register();
			client3.register();
			
			while (client1.discover().size()!=4) {
				continue;
			}
			
			ArrayList<EurekaEntry> entries1 = client1.discover();
			ArrayList<EurekaEntry> entries2 = client2.discover();
			ArrayList<EurekaEntry> entries3 = client3.discover();
	
			String entriesList1="app1 entries:";
			String entriesList2="app2 entries:";
			String entriesList3="app3 entries:";
			
			for (int i=0; i<3; i++) {
				entriesList1=entriesList1+"\n"+entries1.get(i).toString();
				entriesList2=entriesList2+"\n"+entries2.get(i).toString();
				entriesList3=entriesList3+"\n"+entries3.get(i).toString();
			}
			System.out.println(entriesList1);
			System.out.println(entriesList2);
			System.out.println(entriesList3);
			
			client1.unRegister();
			client2.unRegister();
			client3.unRegister();
		}
			
}
