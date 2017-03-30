/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.api;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ResourceDTO;
import org.apache.nifi.web.api.entity.ResourcesEntity;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RESTful endpoint for retrieving system diagnostics.
 */
@Path("/resources")
@Api(
        value = "/resources",
        description = "Provides the resources in this NiFi that can have access/authorization policies."
)
public class ResourceResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    private void authorizeResource() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final Map<String, String> userContext;
        if (!StringUtils.isBlank(user.getClientAddress())) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getResourceResource())
                .identity(user.getIdentity())
                .anonymous(user.isAnonymous())
                .accessAttempt(true)
                .action(RequestAction.READ)
                .userContext(userContext)
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        if (!Result.Approved.equals(result.getResult())) {
            final String message = StringUtils.isNotBlank(result.getExplanation()) ? result.getExplanation() : "Access is denied";
            throw new AccessDeniedException(message);
        }
    }

    /**
     * Gets the available resources that support access/authorization policies.
     *
     * @return A resourcesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Gets the available resources that support access/authorization policies",
            response = ResourcesEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /resources", type = "")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),}
    )
    public Response getResources() {

        authorizeResource();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final List<ResourceDTO> resources = serviceFacade.getResources();

        // create the response
        final ResourcesEntity entity = new ResourcesEntity();
        entity.setResources(resources);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
