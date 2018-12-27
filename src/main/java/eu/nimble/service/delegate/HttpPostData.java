package eu.nimble.service.delegate;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpPostData {
	
			private String hostName;
			private String app;
			private String vipAddress;
			private String secureVipAddress;
			private String ipAddr;
			private String status;
			private Port port;
			private Port securePort;
			private String healthCheckUrl;
			private String statusPageUrl;
			private String homePageUrl;
			private DataCenterInfo dataCenterInfo;
			
			public HttpPostData(String hostName, String app, String ipAddr) {
				this.hostName = hostName;
				this.app = app;
				this.vipAddress = "com.automationrhapsody.eureka.app";
				this.secureVipAddress = "com.automationrhapsody.eureka.app";
				this.ipAddr = ipAddr;
				this.status = "STARTING";
				this.port = new Port("8080", "true");
				this.securePort = new Port("8443", "true");
				this.healthCheckUrl = hostName + ":8080/healthcheck";
				this.statusPageUrl = hostName + ":8080/status";
				this.homePageUrl = hostName + ":8080";
				this.dataCenterInfo = new DataCenterInfo("com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo", "MyOwn");
			}
			
			class DataCenterInfo {
				private String clazz;
				private String name;
				
				public DataCenterInfo(String clazz, String name) {
					this.clazz = clazz;
					this.name = name;
				}
				
				@JsonProperty("@class")
				public String getClazz() {
					return clazz;
				}
				
				public String getName() {
					return name;
				}
			}
			
			class Port {
				private String dollar;
				private String enabled;
				
				public Port(String dollar, String enabled) {
					this.dollar = dollar;
					this.enabled = enabled;
				}
				
				@JsonProperty("@enabled")
				public String getEnabled() {
					return enabled;
				}
				
				public String get$() {
					return dollar;
				}
			}
			public String getHostName() {
				return hostName;
			}

			public String getApp() {
				return app;
			}

			public String getVipAddress() {
				return vipAddress;
			}

			public String getSecureVipAddress() {
				return secureVipAddress;
			}

			public String getIpAddr() {
				return ipAddr;
			}

			public String getStatus() {
				return status;
			}

			public Port getPort() {
				return port;
			}

			public Port getSecurePort() {
				return securePort;
			}

			public String getHealthCheckUrl() {
				return healthCheckUrl;
			}

			public String getStatusPageUrl() {
				return statusPageUrl;
			}

			public String getHomePageUrl() {
				return homePageUrl;
			}

			public DataCenterInfo getDataCenterInfo() {
				return dataCenterInfo;
			}
			
			
}
