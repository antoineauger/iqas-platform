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
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO;
import org.apache.nifi.web.api.entity.LineageEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.ProvenanceOptionsEntity;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/**
 * RESTful endpoint for querying data provenance.
 */
@Path("/provenance")
@Api(
    value = "/provenance",
    description = "Endpoint for accessing data flow provenance."
)
public class ProvenanceResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified provenance.
     */
    private ProvenanceDTO populateRemainingProvenanceContent(ProvenanceDTO provenance) {
        provenance.setUri(generateResourceUri("provenance", provenance.getId()));
        return provenance;
    }

    /**
     * Populates the uri for the specified lineage.
     */
    private LineageDTO populateRemainingLineageContent(LineageDTO lineage) {
        lineage.setUri(generateResourceUri("provenance", "lineage", lineage.getId()));
        return lineage;
    }

    private void authorizeProvenanceRequest() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final Map<String,String> userContext;
        if (!StringUtils.isBlank(user.getClientAddress())) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
            .resource(ResourceFactory.getProvenanceResource())
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
     * Gets the provenance search options for this NiFi.
     *
     * @return A provenanceOptionsEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("search-options")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets the searchable attributes for provenance events",
            response = ProvenanceOptionsEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getSearchOptions() {

        authorizeProvenanceRequest();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get provenance search options
        final ProvenanceOptionsDTO searchOptions = serviceFacade.getProvenanceSearchOptions();

        // create the response entity
        final ProvenanceOptionsEntity entity = new ProvenanceOptionsEntity();
        entity.setProvenanceOptions(searchOptions);

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    /**
     * Creates provenance using the specified query criteria.
     *
     * @param httpServletRequest request
     * @param provenanceEntity A provenanceEntity
     * @return A provenanceEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("") // necessary due to bug in swagger
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Submits a provenance query",
            notes = "Provenance queries may be long running so this endpoint submits a request. The response will include the "
                    + "current state of the query. If the request is not completed the URI in the response can be used at a "
                    + "later time to get the updated state of the query. Once the query has completed the provenance request "
                    + "should be deleted by the client who originally submitted it.",
            response = ProvenanceEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response submitProvenanceRequest(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The provenance query details.",
                    required = true
            ) ProvenanceEntity provenanceEntity) {

        authorizeProvenanceRequest();

        // check the request
        if (provenanceEntity == null) {
            provenanceEntity = new ProvenanceEntity();
        }

        // get the provenance
        ProvenanceDTO provenanceDto = provenanceEntity.getProvenance();
        if (provenanceDto == null) {
            provenanceDto = new ProvenanceDTO();
            provenanceEntity.setProvenance(provenanceDto);
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // determine where this request should be sent
            if (provenanceDto.getRequest() == null || provenanceDto.getRequest().getClusterNodeId() == null) {
                // replicate to all nodes
                return replicate(HttpMethod.POST, provenanceEntity, headersToOverride);
            } else {
                return replicate(HttpMethod.POST, provenanceEntity, provenanceDto.getRequest().getClusterNodeId(), headersToOverride);
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // ensure the id is the same across the cluster
        final String provenanceId = generateUuid();

        // set the provenance id accordingly
        provenanceDto.setId(provenanceId);

        // submit the provenance request
        final ProvenanceDTO dto = serviceFacade.submitProvenance(provenanceDto);
        populateRemainingProvenanceContent(dto);

        // set the cluster id if necessary
        if (provenanceDto.getRequest() != null && provenanceDto.getRequest().getClusterNodeId() != null) {
            dto.getRequest().setClusterNodeId(provenanceDto.getRequest().getClusterNodeId());
        }

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setProvenance(dto);

        // generate the response
        return clusterContext(generateCreatedResponse(URI.create(dto.getUri()), entity)).build();
    }

    /**
     * Gets the provenance with the specified id.
     *
     * @param id The id of the provenance
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @return A provenanceEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets a provenance query",
            response = ProvenanceEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getProvenance(
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The id of the provenance query.",
                    required = true
            )
            @PathParam("id") final String id) {

        authorizeProvenanceRequest();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                // replicate to all nodes
                return replicate(HttpMethod.GET);
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the provenance
        final ProvenanceDTO dto = serviceFacade.getProvenance(id);
        dto.getRequest().setClusterNodeId(clusterNodeId);
        populateRemainingProvenanceContent(dto);

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setProvenance(dto);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Deletes the provenance with the specified id.
     *
     * @param httpServletRequest request
     * @param id The id of the provenance
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @return A provenanceEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Deletes a provenance query",
            response = ProvenanceEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response deleteProvenance(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The id of the provenance query.",
                    required = true
            )
            @PathParam("id") final String id) {

        authorizeProvenanceRequest();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                // replicate to all nodes
                return replicate(HttpMethod.DELETE);
            } else {
                return replicate(HttpMethod.DELETE, clusterNodeId);
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the provenance
        serviceFacade.deleteProvenance(id);

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Submits a lineage request based on an event or a flowfile uuid.
     *
     * When querying for the lineage of an event you must specify the eventId and the eventDirection. The eventDirection must be 'parents' or 'children' and specifies whether we are going up or down
     * the flowfile ancestry. The uuid cannot be specified in these cases.
     *
     * When querying for the lineage of a flowfile you must specify the uuid. The eventId and eventDirection cannot be specified in this case.
     *
     * @param httpServletRequest request
     * @param lineageEntity A lineageEntity
     * @return A lineageEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lineage")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Submits a lineage query",
            notes = "Lineage queries may be long running so this endpoint submits a request. The response will include the "
                    + "current state of the query. If the request is not completed the URI in the response can be used at a "
                    + "later time to get the updated state of the query. Once the query has completed the lineage request "
                    + "should be deleted by the client who originally submitted it.",
            response = LineageEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response submitLineageRequest(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The lineage query details.",
                    required = true
            ) final LineageEntity lineageEntity) {

        authorizeProvenanceRequest();

        if (lineageEntity == null || lineageEntity.getLineage() == null || lineageEntity.getLineage().getRequest() == null) {
            throw new IllegalArgumentException("Lineage request must be specified.");
        }

        // ensure the request is well formed
        final LineageDTO lineageDto = lineageEntity.getLineage();
        final LineageRequestDTO requestDto = lineageDto.getRequest();

        // ensure the type has been specified
        if (requestDto.getLineageRequestType() == null) {
            throw new IllegalArgumentException("The type of lineage request must be specified.");
        }

        // validate the remainder of the request
        switch (requestDto.getLineageRequestType()) {
            case CHILDREN:
            case PARENTS:
                // ensure the event has been specified
                if (requestDto.getEventId() == null) {
                    throw new IllegalArgumentException("The event id must be specified when the event type is PARENTS or CHILDREN.");
                }
                break;
            case FLOWFILE:
                // ensure the uuid has been specified
                if (requestDto.getUuid() == null) {
                    throw new IllegalArgumentException("The flowfile uuid must be specified when the event type is FLOWFILE.");
                }
                break;
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            if (requestDto.getClusterNodeId() == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);
            return replicate(HttpMethod.POST, lineageEntity, requestDto.getClusterNodeId(), headersToOverride);
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // get the provenance event
        final LineageDTO dto = serviceFacade.submitLineage(lineageDto);
        dto.getRequest().setClusterNodeId(requestDto.getClusterNodeId());
        populateRemainingLineageContent(dto);

        // create a response entity
        final LineageEntity entity = new LineageEntity();
        entity.setLineage(dto);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Gets the lineage with the specified id.
     *
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @param id The id of the lineage
     * @return A lineageEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lineage/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets a lineage query",
            response = LineageEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getLineage(
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The id of the lineage query.",
                    required = true
            )
            @PathParam("id") final String id) {

        authorizeProvenanceRequest();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, clusterNodeId);
        }

        // get the lineage
        final LineageDTO dto = serviceFacade.getLineage(id);
        dto.getRequest().setClusterNodeId(clusterNodeId);
        populateRemainingLineageContent(dto);

        // create the response entity
        final LineageEntity entity = new LineageEntity();
        entity.setLineage(dto);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Deletes the lineage with the specified id.
     *
     * @param httpServletRequest request
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @param id The id of the lineage
     * @return A lineageEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lineage/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Deletes a lineage query",
            response = LineageEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response deleteLineage(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The id of the lineage query.",
                    required = true
            )
            @PathParam("id") final String id) {

        authorizeProvenanceRequest();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE, clusterNodeId);
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the lineage
        serviceFacade.deleteLineage(id);

        // create the response entity
        final LineageEntity entity = new LineageEntity();

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
