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
		responseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
		responseContext.getHeaders().add("Access-Control-Allow-Credentials", "true");
		responseContext.getHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization, federationId, initiatorFederationId,responderFederationId");
		responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH");
	}
}
