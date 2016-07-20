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
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;

/**
 * RESTful endpoint for managing access policies.
 */
@Path("/policies")
@Api(
        value = "/policies",
        description = "Endpoint for managing access policies."
)
public class AccessPolicyResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public AccessPolicyResource(NiFiServiceFacade serviceFacade, Authorizer authorizer, NiFiProperties properties, RequestReplicator requestReplicator, ClusterCoordinator clusterCoordinator) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
    }

    /**
     * Populates the uri for the specified access policy.
     *
     * @param accessPolicyEntity accessPolicyEntity
     * @return accessPolicyEntity
     */
    public AccessPolicyEntity populateRemainingAccessPolicyEntityContent(AccessPolicyEntity accessPolicyEntity) {
        if (accessPolicyEntity.getComponent() != null) {
            accessPolicyEntity.setUri(generateResourceUri("policies", accessPolicyEntity.getId()));
        }
        return accessPolicyEntity;
    }

    // -----------------
    // get access policy
    // -----------------

    /**
     * Retrieves the specified access policy.
     *
     * @return An accessPolicyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{action}/{resource: .+}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets an access policy",
            response = AccessPolicyEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getAccessPolicyForResource(
            @ApiParam(
                    value = "The request action.",
                    allowableValues = "read, write",
                    required = true
            ) @PathParam("action") final String action,
            @ApiParam(
                    value = "The resource of the policy.",
                    required = true
            ) @PathParam("resource") String rawResource) {

        // parse the action and resource type
        final RequestAction requestAction = RequestAction.valueOfValue(action);
        final String resource = "/" + rawResource;

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable accessPolicy = lookup.getAccessPolicyByResource(resource);
            accessPolicy.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the access policy
        final AccessPolicyEntity entity = serviceFacade.getAccessPolicy(requestAction, resource);
        populateRemainingAccessPolicyEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    // -----------------------
    // manage an access policy
    // -----------------------

    /**
     * Creates a new access policy.
     *
     * @param httpServletRequest request
     * @param accessPolicyEntity An accessPolicyEntity.
     * @return An accessPolicyEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates an access policy",
            response = AccessPolicyEntity.class,
            authorizations = {
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response createAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The access policy configuration details.",
                    required = true
            ) final AccessPolicyEntity accessPolicyEntity) {

        if (accessPolicyEntity == null || accessPolicyEntity.getComponent() == null) {
            throw new IllegalArgumentException("Access policy details must be specified.");
        }

        if (accessPolicyEntity.getRevision() == null || (accessPolicyEntity.getRevision().getVersion() == null || accessPolicyEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Policy.");
        }

        final AccessPolicyDTO requestAccessPolicy = accessPolicyEntity.getComponent();
        if (requestAccessPolicy.getId() != null) {
            throw new IllegalArgumentException("Access policy ID cannot be specified.");
        }

        if (requestAccessPolicy.getResource() == null) {
            throw new IllegalArgumentException("Access policy resource must be specified.");
        }

        // ensure this is a valid action
        RequestAction.valueOfValue(requestAccessPolicy.getAction());

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, accessPolicyEntity);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable accessPolicies = lookup.getAccessPolicyByResource(requestAccessPolicy.getResource());
                accessPolicies.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the access policy id as appropriate
        accessPolicyEntity.getComponent().setId(generateUuid());

        // get revision from the config
        final RevisionDTO revisionDTO = accessPolicyEntity.getRevision();
        Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), accessPolicyEntity.getComponent().getId());

        // create the access policy and generate the json
        final AccessPolicyEntity entity = serviceFacade.createAccessPolicy(revision, accessPolicyEntity.getComponent());
        populateRemainingAccessPolicyEntityContent(entity);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified access policy.
     *
     * @param id The id of the access policy to retrieve
     * @return An accessPolicyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets an access policy",
            response = AccessPolicyEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getAccessPolicy(
            @ApiParam(
                    value = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            Authorizable authorizable  = lookup.getAccessPolicyById(id);
            authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the access policy
        final AccessPolicyEntity entity = serviceFacade.getAccessPolicy(id);
        populateRemainingAccessPolicyEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates an access policy.
     *
     * @param httpServletRequest request
     * @param id                 The id of the access policy to update.
     * @param accessPolicyEntity An accessPolicyEntity.
     * @return An accessPolicyEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a access policy",
            response = AccessPolicyEntity.class,
            authorizations = {
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response updateAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The access policy configuration details.",
                    required = true
            ) final AccessPolicyEntity accessPolicyEntity) {

        if (accessPolicyEntity == null || accessPolicyEntity.getComponent() == null) {
            throw new IllegalArgumentException("Access policy details must be specified.");
        }

        if (accessPolicyEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final AccessPolicyDTO accessPolicyDTO = accessPolicyEntity.getComponent();
        if (!id.equals(accessPolicyDTO.getId())) {
            throw new IllegalArgumentException(String.format("The access policy id (%s) in the request body does not equal the "
                    + "access policy id of the requested resource (%s).", accessPolicyDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, accessPolicyEntity);
        }

        // Extract the revision
        final Revision revision = getRevision(accessPolicyEntity, id);
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    Authorizable authorizable  = lookup.getAccessPolicyById(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                () -> {
                    // update the access policy
                    final AccessPolicyEntity entity = serviceFacade.updateAccessPolicy(revision, accessPolicyDTO);
                    populateRemainingAccessPolicyEntityContent(entity);

                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }

    /**
     * Removes the specified access policy.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the access policy to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes an access policy",
            response = AccessPolicyEntity.class,
            authorizations = {
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response removeAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The revision is used to verify the client is working with the latest version of the flow.",
                    required = false
            )
            @QueryParam(VERSION) final LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @ApiParam(
                    value = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        // handle expects request (usually from the cluster manager)
        final Revision revision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    final Authorizable accessPolicy = lookup.getAccessPolicyById(id);
                    accessPolicy.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                },
                () -> {
                    // delete the specified access policy
                    final AccessPolicyEntity entity = serviceFacade.deleteAccessPolicy(revision, id);
                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }
}
