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

import com.sun.jersey.api.core.ResourceContext;
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
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.search.NodeSearchResultDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ClusterSearchResultsEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;

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
import java.util.ArrayList;
import java.util.List;

/**
 * RESTful endpoint for managing a Flow Controller.
 */
@Path("/controller")
@Api(
    value = "/controller",
    description = "Provides realtime command and control of this NiFi instance"
)
public class ControllerResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    private ReportingTaskResource reportingTaskResource;
    private ControllerServiceResource controllerServiceResource;

    @Context
    private ResourceContext resourceContext;

    /**
     * Authorizes access to the flow.
     */
    private void authorizeController(final RequestAction action) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getControllerResource())
                .identity(user.getIdentity())
                .anonymous(user.isAnonymous())
                .accessAttempt(true)
                .action(action)
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        if (!Result.Approved.equals(result.getResult())) {
            final String message = StringUtils.isNotBlank(result.getExplanation()) ? result.getExplanation() : "Access is denied";
            throw new AccessDeniedException(message);
        }
    }

    /**
     * Creates a new archive of this flow controller. Note, this is a POST operation that returns a URI that is not representative of the thing that was actually created. The archive that is created
     * cannot be referenced at a later time, therefore there is no corresponding URI. Instead the request URI is returned.
     *
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest request
     * @return A processGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("archive")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a new archive of this NiFi flow configuration",
            notes = "This POST operation returns a URI that is not representative of the thing "
            + "that was actually created. The archive that is created cannot be referenced "
            + "at a later time, therefore there is no corresponding URI. Instead the "
            + "request URI is returned.",
            response = ProcessGroupEntity.class,
            authorizations = {
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response createArchive(@Context final HttpServletRequest httpServletRequest) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                authorizeController(RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // create the archive
        final ProcessGroupEntity entity = serviceFacade.createArchive();

        // generate the response
        final URI uri = URI.create(generateResourceUri("controller", "archive"));
        return clusterContext(generateCreatedResponse(uri, entity)).build();
    }

    /**
     * Retrieves the configuration for this NiFi.
     *
     * @return A controllerConfigurationEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi Controller",
            response = ControllerConfigurationEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN"),
                @Authorization(value = "ROLE_NIFI", type = "ROLE_NIFI")
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
    public Response getControllerConfig() {

        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerConfigurationEntity entity = serviceFacade.getControllerConfiguration();
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Update the configuration for this NiFi.
     *
     * @param httpServletRequest request
     * @param configEntity A controllerConfigurationEntity.
     * @return A controllerConfigurationEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi",
            response = ControllerConfigurationEntity.class,
            authorizations = {
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response updateControllerConfig(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller configuration.",
                    required = true
            ) final ControllerConfigurationEntity configEntity) {

        if (configEntity == null || configEntity.getControllerConfiguration() == null) {
            throw new IllegalArgumentException("Controller configuration must be specified");
        }

        if (configEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, configEntity);
        }

        final Revision revision = getRevision(configEntity.getRevision(), FlowController.class.getSimpleName());
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                () -> {
                    final ControllerConfigurationEntity entity = serviceFacade.updateControllerConfiguration(revision, configEntity.getControllerConfiguration());
                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }

    // ---------------
    // reporting tasks
    // ---------------

    /**
     * Creates a new Reporting Task.
     *
     * @param httpServletRequest request
     * @param reportingTaskEntity A reportingTaskEntity.
     * @return A reportingTaskEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a new reporting task",
        response = ReportingTaskEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response createReportingTask(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The reporting task configuration details.",
            required = true
        ) final ReportingTaskEntity reportingTaskEntity) {

        if (reportingTaskEntity == null || reportingTaskEntity.getComponent() == null) {
            throw new IllegalArgumentException("Reporting task details must be specified.");
        }

        if (reportingTaskEntity.getRevision() == null || (reportingTaskEntity.getRevision().getVersion() == null || reportingTaskEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Reporting task.");
        }

        if (reportingTaskEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Reporting task ID cannot be specified.");
        }

        if (StringUtils.isBlank(reportingTaskEntity.getComponent().getType())) {
            throw new IllegalArgumentException("The type of reporting task to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, reportingTaskEntity);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                authorizeController(RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        reportingTaskEntity.getComponent().setId(generateUuid());

        // create the reporting task and generate the json
        final Revision revision = getRevision(reportingTaskEntity, reportingTaskEntity.getComponent().getId());
        final ReportingTaskEntity entity = serviceFacade.createReportingTask(revision, reportingTaskEntity.getComponent());
        reportingTaskResource.populateRemainingReportingTaskEntityContent(entity);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
    }

    // -------------------
    // controller services
    // -------------------

    /**
     * Creates a new Controller Service.
     *
     * @param httpServletRequest request
     * @param controllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-services")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a new controller service",
        response = ControllerServiceEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response createControllerService(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The controller service configuration details.",
            required = true
        ) final ControllerServiceEntity controllerServiceEntity) {

        if (controllerServiceEntity == null || controllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (controllerServiceEntity.getRevision() == null || (controllerServiceEntity.getRevision().getVersion() == null || controllerServiceEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Controller service.");
        }

        if (controllerServiceEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Controller service ID cannot be specified.");
        }

        if (StringUtils.isBlank(controllerServiceEntity.getComponent().getType())) {
            throw new IllegalArgumentException("The type of controller service to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                authorizeController(RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        controllerServiceEntity.getComponent().setId(generateUuid());

        // create the controller service and generate the json
        final Revision revision = getRevision(controllerServiceEntity, controllerServiceEntity.getComponent().getId());
        final ControllerServiceEntity entity = serviceFacade.createControllerService(revision, null, controllerServiceEntity.getComponent());
        controllerServiceResource.populateRemainingControllerServiceContent(entity.getComponent());

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
    }

    // -------
    // cluster
    // -------

    /**
     * Gets the contents of this NiFi cluster. This includes all nodes and their status.
     *
     * @return A clusterEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the contents of the cluster",
            notes = "Returns the contents of the cluster including all nodes and their status.",
            response = ClusterEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "DFM", type = "ROLE_DFM"),
                    @Authorization(value = "Admin", type = "ROLE_ADMIN")
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
    public Response getCluster() {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        final ClusterDTO dto = serviceFacade.getCluster();

        // create entity
        final ClusterEntity entity = new ClusterEntity();
        entity.setCluster(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Searches the cluster for a node with a given address.
     *
     * @param value Search value that will be matched against a node's address
     * @return Nodes that match the specified criteria
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/search-results")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Searches the cluster for a node with the specified address",
            response = ClusterSearchResultsEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "DFM", type = "ROLE_DFM"),
                    @Authorization(value = "Admin", type = "ROLE_ADMIN")
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
    public Response searchCluster(
            @ApiParam(
                    value = "Node address to search for.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        final List<NodeSearchResultDTO> nodeMatches = new ArrayList<>();

        // get the nodes in the cluster
        final ClusterDTO cluster = serviceFacade.getCluster();

        // check each to see if it matches the search term
        for (NodeDTO node : cluster.getNodes()) {
            // ensure the node is connected
            if (!NodeConnectionState.CONNECTED.name().equals(node.getStatus())) {
                continue;
            }

            // determine the current nodes address
            final String address = node.getAddress() + ":" + node.getApiPort();

            // count the node if there is no search or it matches the address
            if (StringUtils.isBlank(value) || StringUtils.containsIgnoreCase(address, value)) {
                final NodeSearchResultDTO nodeMatch = new NodeSearchResultDTO();
                nodeMatch.setId(node.getNodeId());
                nodeMatch.setAddress(address);
                nodeMatches.add(nodeMatch);
            }
        }

        // build the response
        ClusterSearchResultsEntity results = new ClusterSearchResultsEntity();
        results.setNodeResults(nodeMatches);

        // generate an 200 - OK response
        return noCache(Response.ok(results)).build();
    }

    /**
     * Gets the contents of the specified node in this NiFi cluster.
     *
     * @param id The node id.
     * @return A nodeEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a node in the cluster",
            response = NodeEntity.class,
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
    public Response getNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        // get the specified relationship
        final NodeDTO dto = serviceFacade.getNode(id);

        // create the response entity
        final NodeEntity entity = new NodeEntity();
        entity.setNode(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates the contents of the specified node in this NiFi cluster.
     *
     * @param id The id of the node
     * @param nodeEntity A nodeEntity
     * @return A nodeEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Updates a node in the cluster",
            response = NodeEntity.class,
            authorizations = {
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
    public Response updateNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The node configuration. The only configuration that will be honored at this endpoint is the status or primary flag.",
                    required = true
            ) NodeEntity nodeEntity) {

        authorizeController(RequestAction.WRITE);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (nodeEntity == null || nodeEntity.getNode() == null) {
            throw new IllegalArgumentException("Node details must be specified.");
        }

        // get the request node
        final NodeDTO requestNodeDTO = nodeEntity.getNode();
        if (!id.equals(requestNodeDTO.getNodeId())) {
            throw new IllegalArgumentException(String.format("The node id (%s) in the request body does "
                    + "not equal the node id of the requested resource (%s).", requestNodeDTO.getNodeId(), id));
        }

        // update the node
        final NodeDTO node = serviceFacade.updateNode(requestNodeDTO);

        // create the response entity
        NodeEntity entity = new NodeEntity();
        entity.setNode(node);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Removes the specified from this NiFi cluster.
     *
     * @param id The id of the node
     * @return A nodeEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Removes a node from the cluster",
            response = NodeEntity.class,
            authorizations = {
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
    public Response deleteNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        authorizeController(RequestAction.WRITE);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        serviceFacade.deleteNode(id);

        // create the response entity
        final NodeEntity entity = new NodeEntity();

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -------
    // history
    // -------

    /**
     * Deletes flow history from the specified end date.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param endDate The end date for the purge action.
     * @return A historyEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history")
    // TODO - @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Purges history",
            response = HistoryEntity.class,
            authorizations = {
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
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
    public Response deleteHistory(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Purge actions before this date/time.",
                    required = true
            )
            @QueryParam("endDate") DateTimeParameter endDate) {

        // ensure the end date is specified
        if (endDate == null) {
            throw new IllegalArgumentException("The end date must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                authorizeController(RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // purge the actions
        serviceFacade.deleteActions(endDate.getDateTime());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final HistoryEntity entity = new HistoryEntity();

        // generate the response
        return generateOkResponse(entity).build();
    }

    // setters
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setReportingTaskResource(final ReportingTaskResource reportingTaskResource) {
        this.reportingTaskResource = reportingTaskResource;
    }

    public void setControllerServiceResource(final ControllerServiceResource controllerServiceResource) {
        this.controllerServiceResource = controllerServiceResource;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
