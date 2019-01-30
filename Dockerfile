FROM tomcat
MAINTAINER idanz@il.ibm.com

RUN rm -rf /usr/local/tomcat/webapps/*
ADD ./target/delegate-service.war /usr/local/tomcat/webapps/ROOT.war
COPY ./WEB-INF/web.xml /usr/local/tomcat/conf/web.xml

#ENV eureka.environment dev
CMD ["catalina.sh", "run"]
