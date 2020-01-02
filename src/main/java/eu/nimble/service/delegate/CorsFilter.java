package eu.nimble.service.delegate;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

/**
 * CORS Filter - a class that handles adding CORS headers to all Http responses.
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 06/16/2019.
 */

@Provider
public class CorsFilter implements ContainerResponseFilter {
	@Override
	public void filter(ContainerRequestContext requestContext,
					   ContainerResponseContext responseContext) throws IOException {
		if(!responseContext.getHeaders().containsKey("Access-Control-Allow-Origin")){
			responseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
		}
		if(!responseContext.getHeaders().containsKey("Access-Control-Allow-Credentials")){
			responseContext.getHeaders().add("Access-Control-Allow-Credentials", "true");
		}
		if(!responseContext.getHeaders().containsKey("Access-Control-Allow-Headers")){
			responseContext.getHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization, federationId, initiatorFederationId,responderFederationId");
		}
		if(!responseContext.getHeaders().containsKey("Access-Control-Allow-Methods")){
			responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH");
		}
	}
}
