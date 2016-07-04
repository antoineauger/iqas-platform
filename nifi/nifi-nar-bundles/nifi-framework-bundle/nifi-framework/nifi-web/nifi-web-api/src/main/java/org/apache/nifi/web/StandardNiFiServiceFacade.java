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
package org.apache.nifi.web;

import com.google.common.collect.Sets;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.CountersSnapshotDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.FlowConfigurationDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PreviousValueDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.PropertyHistoryDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.ResourceDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.FlowConfigurationEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.LabelDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.apache.nifi.web.dao.TemplateDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;
import org.apache.nifi.web.revision.DeleteRevisionTask;
import org.apache.nifi.web.revision.ExpiredRevisionClaimException;
import org.apache.nifi.web.revision.ReadOnlyRevisionCallback;
import org.apache.nifi.web.revision.RevisionClaim;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.revision.RevisionUpdate;
import org.apache.nifi.web.revision.StandardRevisionClaim;
import org.apache.nifi.web.revision.StandardRevisionUpdate;
import org.apache.nifi.web.revision.UpdateRevisionTask;
import org.apache.nifi.web.util.SnippetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of NiFiServiceFacade that performs revision checking.
 */
public class StandardNiFiServiceFacade implements NiFiServiceFacade {
    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiServiceFacade.class);

    // nifi core components
    private ControllerFacade controllerFacade;
    private SnippetUtils snippetUtils;

    // revision manager
    private RevisionManager revisionManager;
    private BulletinRepository bulletinRepository;

    // data access objects
    private ProcessorDAO processorDAO;
    private ProcessGroupDAO processGroupDAO;
    private RemoteProcessGroupDAO remoteProcessGroupDAO;
    private LabelDAO labelDAO;
    private FunnelDAO funnelDAO;
    private SnippetDAO snippetDAO;
    private PortDAO inputPortDAO;
    private PortDAO outputPortDAO;
    private ConnectionDAO connectionDAO;
    private ControllerServiceDAO controllerServiceDAO;
    private ReportingTaskDAO reportingTaskDAO;
    private TemplateDAO templateDAO;
    private UserDAO userDAO;
    private UserGroupDAO userGroupDAO;
    private AccessPolicyDAO accessPolicyDAO;
    private ClusterCoordinator clusterCoordinator;
    private HeartbeatMonitor heartbeatMonitor;

    // administrative services
    private AuditService auditService;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;
    private EntityFactory entityFactory;

    private Authorizer authorizer;

    private AuthorizableLookup authorizableLookup;

    // -----------------------------------------
    // Synchronization methods
    // -----------------------------------------

    @Override
    public void authorizeAccess(final AuthorizeAccess authorizeAccess) {
        authorizeAccess.authorize(authorizableLookup);
    }

    @Override
    public void claimRevision(final Revision revision, final NiFiUser user) {
        revisionManager.requestClaim(revision, user);
    }

    @Override
    public void claimRevisions(final Set<Revision> revisions, final NiFiUser user) {
        revisionManager.requestClaim(revisions, user);
    }

    @Override
    public void cancelRevision(final Revision revision) {
        revisionManager.cancelClaim(revision);
    }

    @Override
    public void cancelRevisions(final Set<Revision> revisions) {
        revisionManager.cancelClaims(revisions);
    }

    @Override
    public void releaseRevisionClaim(final Revision revision, final NiFiUser user) throws InvalidRevisionException {
        revisionManager.releaseClaim(new StandardRevisionClaim(revision), user);
    }

    @Override
    public void releaseRevisionClaims(final Set<Revision> revisions, final NiFiUser user) throws InvalidRevisionException {
        revisionManager.releaseClaim(new StandardRevisionClaim(revisions), user);
    }

    @Override
    public Set<Revision> getRevisionsFromGroup(final String groupId, final Function<ProcessGroup, Set<String>> getComponents) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final Set<String> componentIds = revisionManager.get(group.getIdentifier(), rev -> getComponents.apply(group));
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    @Override
    public Set<Revision> getRevisionsFromSnippet(final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final Set<String> componentIds = new HashSet<>();
        componentIds.addAll(snippet.getProcessors().keySet());
        componentIds.addAll(snippet.getFunnels().keySet());
        componentIds.addAll(snippet.getLabels().keySet());
        componentIds.addAll(snippet.getConnections().keySet());
        componentIds.addAll(snippet.getInputPorts().keySet());
        componentIds.addAll(snippet.getOutputPorts().keySet());
        componentIds.addAll(snippet.getProcessGroups().keySet());
        componentIds.addAll(snippet.getRemoteProcessGroups().keySet());
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    // -----------------------------------------
    // Verification Operations
    // -----------------------------------------

    @Override
    public void verifyListQueue(final String connectionId) {
        connectionDAO.verifyList(connectionId);
    }

    @Override
    public void verifyCreateConnection(final String groupId, final ConnectionDTO connectionDTO) {
        connectionDAO.verifyCreate(groupId, connectionDTO);
    }

    @Override
    public void verifyUpdateConnection(final ConnectionDTO connectionDTO) {
        try {
            // if connection does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (connectionDAO.hasConnection(connectionDTO.getId())) {
                connectionDAO.verifyUpdate(connectionDTO);
            } else {
                connectionDAO.verifyCreate(connectionDTO.getParentGroupId(), connectionDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(connectionDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteConnection(final String connectionId) {
        connectionDAO.verifyDelete(connectionId);
    }

    @Override
    public void verifyDeleteFunnel(final String funnelId) {
        funnelDAO.verifyDelete(funnelId);
    }

    @Override
    public void verifyUpdateInputPort(final PortDTO inputPortDTO) {
        try {
            // if connection does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (inputPortDAO.hasPort(inputPortDTO.getId())) {
                inputPortDAO.verifyUpdate(inputPortDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(inputPortDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteInputPort(final String inputPortId) {
        inputPortDAO.verifyDelete(inputPortId);
    }

    @Override
    public void verifyUpdateOutputPort(final PortDTO outputPortDTO) {
        try {
            // if connection does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (outputPortDAO.hasPort(outputPortDTO.getId())) {
                outputPortDAO.verifyUpdate(outputPortDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(outputPortDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteOutputPort(final String outputPortId) {
        outputPortDAO.verifyDelete(outputPortId);
    }

    @Override
    public void verifyUpdateProcessor(final ProcessorDTO processorDTO) {
        try {
            // if group does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (processorDAO.hasProcessor(processorDTO.getId())) {
                processorDAO.verifyUpdate(processorDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(processorDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteProcessor(final String processorId) {
        processorDAO.verifyDelete(processorId);
    }

    @Override
    public void verifyScheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        try {
            processGroupDAO.verifyScheduleComponents(groupId, state, componentIds);
        } catch (final Exception e) {
            componentIds.forEach(id -> revisionManager.cancelClaim(id));
            throw e;
        }
    }

    @Override
    public void verifyDeleteProcessGroup(final String groupId) {
        processGroupDAO.verifyDelete(groupId);
    }

    @Override
    public void verifyUpdateRemoteProcessGroup(final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        try {
            // if remote group does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId())) {
                remoteProcessGroupDAO.verifyUpdate(remoteProcessGroupDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(remoteProcessGroupDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupInputPort(final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        try {
            remoteProcessGroupDAO.verifyUpdateInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
        } catch (final Exception e) {
            revisionManager.cancelClaim(remoteProcessGroupId);
            throw e;
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupOutputPort(final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        try {
            remoteProcessGroupDAO.verifyUpdateOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
        } catch (final Exception e) {
            revisionManager.cancelClaim(remoteProcessGroupId);
            throw e;
        }
    }

    @Override
    public void verifyDeleteRemoteProcessGroup(final String remoteProcessGroupId) {
        remoteProcessGroupDAO.verifyDelete(remoteProcessGroupId);
    }

    @Override
    public void verifyUpdateControllerService(final ControllerServiceDTO controllerServiceDTO) {
        try {
            // if service does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId())) {
                controllerServiceDAO.verifyUpdate(controllerServiceDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(controllerServiceDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyUpdateControllerServiceReferencingComponents(final String controllerServiceId, final ScheduledState scheduledState, final ControllerServiceState controllerServiceState) {
        try {
            controllerServiceDAO.verifyUpdateReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
        } catch (final Exception e) {
            revisionManager.cancelClaim(controllerServiceId);
            throw e;
        }
    }

    @Override
    public void verifyDeleteControllerService(final String controllerServiceId) {
        controllerServiceDAO.verifyDelete(controllerServiceId);
    }

    @Override
    public void verifyUpdateReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        try {
            // if tasks does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId())) {
                reportingTaskDAO.verifyUpdate(reportingTaskDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(reportingTaskDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteReportingTask(final String reportingTaskId) {
        reportingTaskDAO.verifyDelete(reportingTaskId);
    }

    // -----------------------------------------
    // Write Operations
    // -----------------------------------------
    @Override
    public AccessPolicyEntity updateAccessPolicy(final Revision revision, final AccessPolicyDTO accessPolicyDTO) {
        final Authorizable accessPolicyAuthorizable = authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDTO.getId());
        final RevisionUpdate<AccessPolicyDTO> snapshot = updateComponent(revision,
                accessPolicyAuthorizable,
                () -> accessPolicyDAO.updateAccessPolicy(accessPolicyDTO),
                accessPolicy -> {
                    final Set<TenantEntity> users = accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
                    final Set<TenantEntity> userGroups = accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
                    return dtoFactory.createAccessPolicyDto(accessPolicy, userGroups, users);
                });

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(accessPolicyAuthorizable);
        return entityFactory.createAccessPolicyEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    @Override
    public UserEntity updateUser(final Revision revision, final UserDTO userDTO) {
        final Authorizable usersAuthorizable = authorizableLookup.getTenantAuthorizable();
        final RevisionUpdate<UserDTO> snapshot = updateComponent(revision,
                usersAuthorizable,
                () -> userDAO.updateUser(userDTO),
                user -> dtoFactory.createUserDto(user, user.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet())));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(usersAuthorizable);
        return entityFactory.createUserEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    @Override
    public UserGroupEntity updateUserGroup(final Revision revision, final UserGroupDTO userGroupDTO) {
        final Authorizable userGroupsAuthorizable = authorizableLookup.getTenantAuthorizable();
        final RevisionUpdate<UserGroupDTO> snapshot = updateComponent(revision,
                userGroupsAuthorizable,
                () -> userGroupDAO.updateUserGroup(userGroupDTO),
                userGroup -> dtoFactory.createUserGroupDto(userGroup, userGroup.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet())));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(userGroupsAuthorizable);
        return entityFactory.createUserGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    @Override
    public ConnectionEntity updateConnection(final Revision revision, final ConnectionDTO connectionDTO) {
        final Connection connectionNode = connectionDAO.getConnection(connectionDTO.getId());

        final RevisionUpdate<ConnectionDTO> snapshot = updateComponent(
                revision,
                connectionNode,
                () -> connectionDAO.updateConnection(connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connectionNode);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionNode.getIdentifier()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public ProcessorEntity updateProcessor(final Revision revision, final ProcessorDTO processorDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
        final RevisionUpdate<ProcessorDTO> snapshot = updateComponent(revision,
                processorNode,
                () -> processorDAO.updateProcessor(processorDTO),
                proc -> dtoFactory.createProcessorDto(proc));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processorNode);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processorNode.getIdentifier()));
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public LabelEntity updateLabel(final Revision revision, final LabelDTO labelDTO) {
        final Label labelNode = labelDAO.getLabel(labelDTO.getId());
        final RevisionUpdate<LabelDTO> snapshot = updateComponent(revision,
                labelNode,
                () -> labelDAO.updateLabel(labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(labelNode);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    @Override
    public FunnelEntity updateFunnel(final Revision revision, final FunnelDTO funnelDTO) {
        final Funnel funnelNode = funnelDAO.getFunnel(funnelDTO.getId());
        final RevisionUpdate<FunnelDTO> snapshot = updateComponent(revision,
                funnelNode,
                () -> funnelDAO.updateFunnel(funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnelNode);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }


    /**
     * Updates a component with the given revision, using the provided supplier to call
     * into the appropriate DAO and the provided function to convert the component into a DTO.
     *
     * @param revision    the current revision
     * @param daoUpdate   a Supplier that will update the component via the appropriate DAO
     * @param dtoCreation a Function to convert a component into a dao
     * @param <D>         the DTO Type of the updated component
     * @param <C>         the Component Type of the updated component
     * @return A RevisionUpdate that represents the new configuration
     */
    private <D, C> RevisionUpdate<D> updateComponent(final Revision revision, final Authorizable authorizable, final Supplier<C> daoUpdate, final Function<C, D> dtoCreation) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        try {
            final RevisionUpdate<D> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(revision), user, new UpdateRevisionTask<D>() {
                @Override
                public RevisionUpdate<D> update() {
                    // get the updated component
                    final C component = daoUpdate.get();

                    // save updated controller
                    controllerFacade.save();

                    final D dto = dtoCreation.apply(component);

                    final Revision updatedRevision = revisionManager.getRevision(revision.getComponentId()).incrementRevision(revision.getClientId());
                    final FlowModification lastModification = new FlowModification(updatedRevision, user.getUserName());
                    return new StandardRevisionUpdate<>(dto, lastModification);
                }
            });

            return updatedComponent;
        } catch (final ExpiredRevisionClaimException erce) {
            throw new InvalidRevisionException("Failed to update component " + authorizable, erce);
        }
    }


    @Override
    public void verifyUpdateSnippet(final SnippetDTO snippetDto, final Set<String> affectedComponentIds) {
        try {
            // if snippet does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (snippetDAO.hasSnippet(snippetDto.getId())) {
                snippetDAO.verifyUpdateSnippetComponent(snippetDto);
            }
        } catch (final Exception e) {
            affectedComponentIds.forEach(id -> revisionManager.cancelClaim(snippetDto.getId()));
            throw e;
        }
    }

    @Override
    public SnippetEntity updateSnippet(final Set<Revision> revisions, final SnippetDTO snippetDto) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revisions);

        final RevisionUpdate<SnippetDTO> snapshot;
        try {
            snapshot = revisionManager.updateRevision(revisionClaim, user, new UpdateRevisionTask<SnippetDTO>() {
                @Override
                public RevisionUpdate<SnippetDTO> update() {
                    // get the updated component
                    final Snippet snippet = snippetDAO.updateSnippetComponents(snippetDto);

                    // drop the snippet
                    snippetDAO.dropSnippet(snippet.getId());

                    // save updated controller
                    controllerFacade.save();

                    // increment the revisions
                    final Set<Revision> updatedRevisions = revisions.stream().map(revision -> {
                        final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                        return currentRevision.incrementRevision(revision.getClientId());
                    }).collect(Collectors.toSet());

                    final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
                    return new StandardRevisionUpdate<>(dto, null, updatedRevisions);
                }
            });
        } catch (final ExpiredRevisionClaimException e) {
            throw new InvalidRevisionException("Failed to update Snippet", e);
        }

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity updateInputPort(final Revision revision, final PortDTO inputPortDTO) {
        final Port inputPortNode = inputPortDAO.getPort(inputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                inputPortNode,
                () -> inputPortDAO.updatePort(inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(inputPortNode);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(inputPortNode.getIdentifier()));
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public PortEntity updateOutputPort(final Revision revision, final PortDTO outputPortDTO) {
        final Port outputPortNode = outputPortDAO.getPort(outputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                outputPortNode,
                () -> outputPortDAO.updatePort(outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(outputPortNode);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(outputPortNode.getIdentifier()));
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public RemoteProcessGroupEntity updateRemoteProcessGroup(final Revision revision, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroup(remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroupNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroupNode.getIdentifier()));
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), updateRevision, accessPolicy, status, bulletins);
    }

    @Override
    public RemoteProcessGroupPortEntity updateRemoteProcessGroupInputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        final RevisionUpdate<RemoteProcessGroupPortDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
                remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, accessPolicy);
    }

    @Override
    public RemoteProcessGroupPortEntity updateRemoteProcessGroupOutputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        final RevisionUpdate<RemoteProcessGroupPortDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
                remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, accessPolicy);
    }

    @Override
    public ProcessGroupEntity updateProcessGroup(final Revision revision, final ProcessGroupDTO processGroupDTO) {
        final ProcessGroup processGroupNode = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final RevisionUpdate<ProcessGroupDTO> snapshot = updateComponent(revision,
                processGroupNode,
                () -> processGroupDAO.updateProcessGroup(processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroupNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processGroupNode.getIdentifier()));
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), updatedRevision, accessPolicy, status, bulletins);
    }

    @Override
    public ScheduleComponentsEntity scheduleComponents(final String processGroupId, final ScheduledState state, final Map<String, Revision> componentRevisions) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionUpdate<ScheduleComponentsEntity> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(componentRevisions.values()), user, new
                UpdateRevisionTask<ScheduleComponentsEntity>() {
                    @Override
                    public RevisionUpdate<ScheduleComponentsEntity> update() {
                        // schedule the components
                        processGroupDAO.scheduleComponents(processGroupId, state, componentRevisions.keySet());

                        // update the revisions
                        final Map<String, Revision> updatedRevisions = new HashMap<>();
                        for (final Revision revision : componentRevisions.values()) {
                            final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                            updatedRevisions.put(revision.getComponentId(), currentRevision.incrementRevision(revision.getClientId()));
                        }

                        // gather details for response
                        final ScheduleComponentsEntity entity = new ScheduleComponentsEntity();
                        entity.setId(processGroupId);
                        entity.setState(state.name());
                        return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                    }
                });

        return updatedComponent.getComponent();
    }

    @Override
    public ControllerConfigurationEntity updateControllerConfiguration(final Revision revision, final ControllerConfigurationDTO controllerConfigurationDTO) {
        final RevisionUpdate<ControllerConfigurationDTO> updatedComponent = updateComponent(
                revision,
                controllerFacade,
                () -> {
                    if (controllerConfigurationDTO.getMaxTimerDrivenThreadCount() != null) {
                        controllerFacade.setMaxTimerDrivenThreadCount(controllerConfigurationDTO.getMaxTimerDrivenThreadCount());
                    }
                    if (controllerConfigurationDTO.getMaxEventDrivenThreadCount() != null) {
                        controllerFacade.setMaxEventDrivenThreadCount(controllerConfigurationDTO.getMaxEventDrivenThreadCount());
                    }

                    return controllerConfigurationDTO;
                },
                controller -> dtoFactory.createControllerConfigurationDto(controllerFacade));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerFacade);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(updatedComponent.getLastModification());
        return entityFactory.createControllerConfigurationEntity(updatedComponent.getComponent(), updateRevision, accessPolicy);
    }

    @Override
    public NodeDTO updateNode(final NodeDTO nodeDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }
        final String userDn = user.getIdentity();

        final NodeIdentifier nodeId = clusterCoordinator.getNodeIdentifier(nodeDTO.getNodeId());
        if (nodeId == null) {
            throw new UnknownNodeException("No node exists with ID " + nodeDTO.getNodeId());
        }


        if (NodeConnectionState.CONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeConnect(nodeId, userDn);
        } else if (NodeConnectionState.DISCONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeDisconnect(nodeId, DisconnectionCode.USER_DISCONNECTED,
                    "User " + userDn + " requested that node be disconnected from cluster");
        }

        return getNode(nodeId);
    }

    @Override
    public CounterDTO updateCounter(final String counterId) {
        return dtoFactory.createCounterDto(controllerFacade.resetCounter(counterId));
    }

    @Override
    public void verifyCanClearProcessorState(final String processorId) {
        try {
            processorDAO.verifyClearState(processorId);
        } catch (final Exception e) {
            revisionManager.cancelClaim(processorId);
            throw e;
        }
    }

    @Override
    public void clearProcessorState(final String processorId) {
        clearComponentState(processorId, () -> processorDAO.clearState(processorId));
    }

    private void clearComponentState(final String componentId, final Runnable clearState) {
        revisionManager.get(componentId, rev -> {
            clearState.run();
            return null;
        });
    }

    @Override
    public void verifyCanClearControllerServiceState(final String controllerServiceId) {
        try {
            controllerServiceDAO.verifyClearState(controllerServiceId);
        } catch (final Exception e) {
            revisionManager.cancelClaim(controllerServiceId);
            throw e;
        }
    }

    @Override
    public void clearControllerServiceState(final String controllerServiceId) {
        clearComponentState(controllerServiceId, () -> controllerServiceDAO.clearState(controllerServiceId));
    }

    @Override
    public void verifyCanClearReportingTaskState(final String reportingTaskId) {
        try {
            reportingTaskDAO.verifyClearState(reportingTaskId);
        } catch (final Exception e) {
            revisionManager.cancelClaim(reportingTaskId);
            throw e;
        }
    }

    @Override
    public void clearReportingTaskState(final String reportingTaskId) {
        clearComponentState(reportingTaskId, () -> reportingTaskDAO.clearState(reportingTaskId));
    }

    @Override
    public ConnectionEntity deleteConnection(final Revision revision, final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ConnectionDTO snapshot = deleteComponent(
                revision,
                connection,
                () -> connectionDAO.deleteConnection(connectionId),
                dtoFactory.createConnectionDto(connection));

        return entityFactory.createConnectionEntity(snapshot, null, null, null);
    }

    @Override
    public DropRequestDTO deleteFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.deleteFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO deleteFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.deleteFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ProcessorEntity deleteProcessor(final Revision revision, final String processorId) {
        final ProcessorNode processor = processorDAO.getProcessor(processorId);
        final ProcessorDTO snapshot = deleteComponent(
                revision,
                processor,
                () -> processorDAO.deleteProcessor(processorId),
                dtoFactory.createProcessorDto(processor));

        return entityFactory.createProcessorEntity(snapshot, null, null, null, null);
    }

    @Override
    public LabelEntity deleteLabel(final Revision revision, final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        final LabelDTO snapshot = deleteComponent(
                revision,
                label,
                () -> labelDAO.deleteLabel(labelId),
                dtoFactory.createLabelDto(label));

        return entityFactory.createLabelEntity(snapshot, null, null);
    }

    @Override
    public UserEntity deleteUser(final Revision revision, final String userId) {
        final User user = userDAO.getUser(userId);
        final Set<TenantEntity> userGroups = user != null ? user.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final UserDTO snapshot = deleteComponent(
                revision,
                authorizableLookup.getTenantAuthorizable(),
                () -> userDAO.deleteUser(userId),
                dtoFactory.createUserDto(user, userGroups));

        return entityFactory.createUserEntity(snapshot, null, null);
    }

    @Override
    public UserGroupEntity deleteUserGroup(final Revision revision, final String userGroupId) {
        final Group userGroup = userGroupDAO.getUserGroup(userGroupId);
        final Set<TenantEntity> users = userGroup != null ? userGroup.getUsers().stream()
                .map(mapUserIdToTenantEntity()).collect(Collectors.toSet()) :
                null;
        final UserGroupDTO snapshot = deleteComponent(
                revision,
                authorizableLookup.getTenantAuthorizable(),
                () -> userGroupDAO.deleteUserGroup(userGroupId),
                dtoFactory.createUserGroupDto(userGroup, users));

        return entityFactory.createUserGroupEntity(snapshot, null, null);
    }

    @Override
    public AccessPolicyEntity deleteAccessPolicy(final Revision revision, final String accessPolicyId) {
        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
        final Set<TenantEntity> userGroups = accessPolicy != null ? accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final Set<TenantEntity> users = accessPolicy != null ? accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final AccessPolicyDTO snapshot = deleteComponent(
                revision,
                authorizableLookup.getAccessPolicyAuthorizable(accessPolicyId),
                () -> accessPolicyDAO.deleteAccessPolicy(accessPolicyId),
                dtoFactory.createAccessPolicyDto(accessPolicy, userGroups,
                        users));

        return entityFactory.createAccessPolicyEntity(snapshot, null, null);
    }

    @Override
    public FunnelEntity deleteFunnel(final Revision revision, final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        final FunnelDTO snapshot = deleteComponent(
                revision,
                funnel,
                () -> funnelDAO.deleteFunnel(funnelId),
                dtoFactory.createFunnelDto(funnel));

        return entityFactory.createFunnelEntity(snapshot, null, null);
    }

    /**
     * Deletes a component using the Optimistic Locking Manager
     *
     * @param revision     the current revision
     * @param deleteAction the action that deletes the component via the appropriate DAO object
     * @return a dto that represents the new configuration
     */
    private <D, C> D deleteComponent(final Revision revision, final Authorizable authorizable, final Runnable deleteAction, final D dto) {
        final RevisionClaim claim = new StandardRevisionClaim(revision);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return revisionManager.deleteRevision(claim, user, new DeleteRevisionTask<D>() {
            @Override
            public D performTask() {
                logger.debug("Attempting to delete component {} with claim {}", authorizable, claim);

                deleteAction.run();

                // save the flow
                controllerFacade.save();
                logger.debug("Deletion of component {} was successful", authorizable);

                return dto;
            }
        });
    }

    @Override
    public void verifyDeleteSnippet(final String snippetId, final Set<String> affectedComponentIds) {
        try {
            snippetDAO.verifyDeleteSnippetComponents(snippetId);
        } catch (final Exception e) {
            affectedComponentIds.forEach(id -> revisionManager.cancelClaim(id));
            throw e;
        }
    }

    @Override
    public SnippetEntity deleteSnippet(final Set<Revision> revisions, final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim claim = new StandardRevisionClaim(revisions);
        final SnippetDTO dto = revisionManager.deleteRevision(claim, user, new DeleteRevisionTask<SnippetDTO>() {
            @Override
            public SnippetDTO performTask() {
                // delete the components in the snippet
                snippetDAO.deleteSnippetComponents(snippetId);

                // drop the snippet
                snippetDAO.dropSnippet(snippetId);

                // save
                controllerFacade.save();

                // create the dto for the snippet that was just removed
                return dtoFactory.createSnippetDto(snippet);
            }
        });

        return entityFactory.createSnippetEntity(dto);
    }

    @Override
    public PortEntity deleteInputPort(final Revision revision, final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        final PortDTO snapshot = deleteComponent(
                revision,
                port,
                () -> inputPortDAO.deletePort(inputPortId),
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, null, null, null);
    }

    @Override
    public PortEntity deleteOutputPort(final Revision revision, final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        final PortDTO snapshot = deleteComponent(
                revision,
                port,
                () -> outputPortDAO.deletePort(outputPortId),
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, null, null, null);
    }

    @Override
    public ProcessGroupEntity deleteProcessGroup(final Revision revision, final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final ProcessGroupDTO snapshot = deleteComponent(
                revision,
                processGroup,
                () -> processGroupDAO.deleteProcessGroup(groupId),
                dtoFactory.createProcessGroupDto(processGroup));

        return entityFactory.createProcessGroupEntity(snapshot, null, null, null, null);
    }

    @Override
    public RemoteProcessGroupEntity deleteRemoteProcessGroup(final Revision revision, final String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final RemoteProcessGroupDTO snapshot = deleteComponent(
                revision,
                remoteProcessGroup,
                () -> remoteProcessGroupDAO.deleteRemoteProcessGroup(remoteProcessGroupId),
                dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        return entityFactory.createRemoteProcessGroupEntity(snapshot, null, null, null, null);
    }

    @Override
    public void deleteTemplate(final String id) {
        // create the template
        templateDAO.deleteTemplate(id);
    }

    @Override
    public ConnectionEntity createConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        final RevisionUpdate<ConnectionDTO> snapshot = createComponent(
                revision,
                connectionDTO,
                () -> connectionDAO.createConnection(groupId, connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final Connection connection = connectionDAO.getConnection(connectionDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionDTO.getId()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public DropRequestDTO createFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.createFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO createFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.createFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ProcessorEntity createProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        final RevisionUpdate<ProcessorDTO> snapshot = createComponent(
                revision,
                processorDTO,
                () -> processorDAO.createProcessor(groupId, processorDTO),
                processor -> dtoFactory.createProcessorDto(processor));

        final ProcessorNode processor = processorDAO.getProcessor(processorDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processor);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorDTO.getId()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processorDTO.getId()));
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public LabelEntity createLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        final RevisionUpdate<LabelDTO> snapshot = createComponent(
                revision,
                labelDTO,
                () -> labelDAO.createLabel(groupId, labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final Label label = labelDAO.getLabel(labelDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    /**
     * Creates a component using the optimistic locking manager.
     *
     * @param componentDto the DTO that will be used to create the component
     * @param daoCreation  A Supplier that will create the NiFi Component to use
     * @param dtoCreation  a Function that will convert the NiFi Component into a corresponding DTO
     * @param <D>          the DTO Type
     * @param <C>          the NiFi Component Type
     * @return a RevisionUpdate that represents the updated configuration
     */
    private <D, C> RevisionUpdate<D> createComponent(final Revision revision, final ComponentDTO componentDto, final Supplier<C> daoCreation, final Function<C, D> dtoCreation) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final String groupId = componentDto.getParentGroupId();

        // read lock on the containing group
        return revisionManager.get(groupId, rev -> {
            // request claim for component to be created... revision already verified (version == 0)
            final RevisionClaim claim = revisionManager.requestClaim(revision, user);
            try {
                // update revision through revision manager
                return revisionManager.updateRevision(claim, user, () -> {
                    // add the component
                    final C component = daoCreation.get();

                    // save the flow
                    controllerFacade.save();

                    final D dto = dtoCreation.apply(component);
                    final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getUserName());
                    return new StandardRevisionUpdate<D>(dto, lastMod);
                });
            } finally {
                // cancel in case of exception... noop if successful
                revisionManager.cancelClaim(revision.getComponentId());
            }
        });
    }


    @Override
    public FunnelEntity createFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        final RevisionUpdate<FunnelDTO> snapshot = createComponent(
                revision,
                funnelDTO,
                () -> funnelDAO.createFunnel(groupId, funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final Funnel funnel = funnelDAO.getFunnel(funnelDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    @Override
    public AccessPolicyEntity createAccessPolicy(final Revision revision, final AccessPolicyDTO accessPolicyDTO) {
        // TODO read lock on users and groups (and resource+action?) while the policy is being created?
        final Authorizable tenantAuthorizable = authorizableLookup.getTenantAuthorizable();
        final String creator = NiFiUserUtils.getNiFiUserName();
        final AccessPolicy newAccessPolicy = accessPolicyDAO.createAccessPolicy(accessPolicyDTO);
        final AccessPolicyDTO newAccessPolicyDto = dtoFactory.createAccessPolicyDto(newAccessPolicy,
                newAccessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()),
                newAccessPolicy.getUsers().stream().map(userId -> {
                    final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
                    return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userDAO.getUser(userId)), userRevision,
                            dtoFactory.createAccessPolicyDto(tenantAuthorizable));
                }).collect(Collectors.toSet()));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(authorizableLookup.getAccessPolicyAuthorizable(newAccessPolicy.getIdentifier()));
        return entityFactory.createAccessPolicyEntity(newAccessPolicyDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), accessPolicy);
    }

    @Override
    public UserEntity createUser(final Revision revision, final UserDTO userDTO) {
        final Authorizable tenantAuthorizable = authorizableLookup.getTenantAuthorizable();
        final String creator = NiFiUserUtils.getNiFiUserName();
        final User newUser = userDAO.createUser(userDTO);
        final UserDTO newUserDto = dtoFactory.createUserDto(newUser, newUser.getGroups().stream()
                .map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(authorizableLookup.getTenantAuthorizable());
        return entityFactory.createUserEntity(newUserDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), accessPolicy);
    }

    @Override
    public UserGroupEntity createUserGroup(final Revision revision, final UserGroupDTO userGroupDTO) {
        final Authorizable tenantAuthorizable = authorizableLookup.getTenantAuthorizable();
        final String creator = NiFiUserUtils.getNiFiUserName();
        final Group newUserGroup = userGroupDAO.createUserGroup(userGroupDTO);
        final UserGroupDTO newUserGroupDto = dtoFactory.createUserGroupDto(newUserGroup, newUserGroup.getUsers().stream()
                .map(userId -> {
                    final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
                    return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userDAO.getUser(userId)), userRevision,
                            dtoFactory.createAccessPolicyDto(tenantAuthorizable));
                }).collect(Collectors.toSet()));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(authorizableLookup.getTenantAuthorizable());
        return entityFactory.createUserGroupEntity(newUserGroupDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), accessPolicy);
    }

    private void validateSnippetContents(final FlowSnippetDTO flow) {
        // validate any processors
        if (flow.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : flow.getProcessors()) {
                final ProcessorNode processorNode = revisionManager.get(processorDTO.getId(), rev -> processorDAO.getProcessor(processorDTO.getId()));
                final Collection<ValidationResult> validationErrors = processorNode.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    processorDTO.setValidationErrors(errors);
                }
            }
        }

        if (flow.getInputPorts() != null) {
            for (final PortDTO portDTO : flow.getInputPorts()) {
                final Port port = revisionManager.get(portDTO.getId(), rev -> inputPortDAO.getPort(portDTO.getId()));
                final Collection<ValidationResult> validationErrors = port.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    portDTO.setValidationErrors(errors);
                }
            }
        }

        if (flow.getOutputPorts() != null) {
            for (final PortDTO portDTO : flow.getOutputPorts()) {
                final Port port = revisionManager.get(portDTO.getId(), rev -> outputPortDAO.getPort(portDTO.getId()));
                final Collection<ValidationResult> validationErrors = port.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    portDTO.setValidationErrors(errors);
                }
            }
        }

        // get any remote process group issues
        if (flow.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteProcessGroupDTO : flow.getRemoteProcessGroups()) {
                final RemoteProcessGroup remoteProcessGroup = revisionManager.get(
                        remoteProcessGroupDTO.getId(), rev -> remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId()));

                if (remoteProcessGroup.getAuthorizationIssue() != null) {
                    remoteProcessGroupDTO.setAuthorizationIssues(Arrays.asList(remoteProcessGroup.getAuthorizationIssue()));
                }
            }
        }
    }

    @Override
    public FlowEntity copySnippet(final String groupId, final String snippetId, final Double originX, final Double originY, final String idGenerationSeed) {
        final FlowDTO flowDto = revisionManager.get(groupId,
                rev -> {
                    // create the new snippet
                    final FlowSnippetDTO snippet = snippetDAO.copySnippet(groupId, snippetId, originX, originY, idGenerationSeed);

                    // save the flow
                    controllerFacade.save();

                    // drop the snippet
                    snippetDAO.dropSnippet(snippetId);

                    // post process new flow snippet
                    return postProcessNewFlowSnippet(groupId, snippet);
                });

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    @Override
    public SnippetEntity createSnippet(final SnippetDTO snippetDTO) {
        final String groupId = snippetDTO.getParentGroupId();
        final RevisionUpdate<SnippetDTO> snapshot = revisionManager.get(groupId, rev -> {
            // add the component
            final Snippet snippet = snippetDAO.createSnippet(snippetDTO);

            // save the flow
            controllerFacade.save();

            final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
            return new StandardRevisionUpdate<SnippetDTO>(dto, null);
        });

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity createInputPort(final Revision revision, final String groupId, final PortDTO inputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                revision,
                inputPortDTO,
                () -> inputPortDAO.createPort(groupId, inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = inputPortDAO.getPort(inputPortDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public PortEntity createOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                revision,
                outputPortDTO,
                () -> outputPortDAO.createPort(groupId, outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = outputPortDAO.getPort(outputPortDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final Revision revision, final String parentGroupId, final ProcessGroupDTO processGroupDTO) {
        final RevisionUpdate<ProcessGroupDTO> snapshot = createComponent(
                revision,
                processGroupDTO,
                () -> processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processGroup.getIdentifier()));
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public RemoteProcessGroupEntity createRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = createComponent(
                revision,
                remoteProcessGroupDTO,
                () -> remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroup);
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroup.getIdentifier()));
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status, bulletins);
    }

    @Override
    public TemplateDTO createTemplate(final String name, final String description, final String snippetId, final String groupId, final Optional<String> idGenerationSeed) {
        // get the specified snippet
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

        // create the template
        final TemplateDTO templateDTO = new TemplateDTO();
        templateDTO.setName(name);
        templateDTO.setDescription(description);
        templateDTO.setTimestamp(new Date());
        templateDTO.setSnippet(snippetUtils.populateFlowSnippet(snippet, true, true));
        templateDTO.setEncodingVersion(TemplateDTO.MAX_ENCODING_VERSION);

        // set the id based on the specified seed
        final String uuid = idGenerationSeed.isPresent() ? (UUID.nameUUIDFromBytes(idGenerationSeed.get().getBytes(StandardCharsets.UTF_8))).toString() : UUID.randomUUID().toString();
        templateDTO.setId(uuid);

        // create the template
        final Template template = templateDAO.createTemplate(templateDTO, groupId);

        // drop the snippet
        snippetDAO.dropSnippet(snippetId);

        return dtoFactory.createTemplateDTO(template);
    }

    @Override
    public TemplateDTO importTemplate(final TemplateDTO templateDTO, final String groupId, final Optional<String> idGenerationSeed) {
        // ensure id is set
        final String uuid = idGenerationSeed.isPresent() ? (UUID.nameUUIDFromBytes(idGenerationSeed.get().getBytes(StandardCharsets.UTF_8))).toString() : UUID.randomUUID().toString();
        templateDTO.setId(uuid);

        // mark the timestamp
        templateDTO.setTimestamp(new Date());

        // import the template
        final Template template = templateDAO.importTemplate(templateDTO, groupId);

        // return the template dto
        return dtoFactory.createTemplateDTO(template);
    }

    /**
     * Post processes a new flow snippet including validation, removing the snippet, and DTO conversion.
     *
     * @param groupId group id
     * @param snippet snippet
     * @return flow dto
     */
    private FlowDTO postProcessNewFlowSnippet(final String groupId, final FlowSnippetDTO snippet) {
        // validate the new snippet
        validateSnippetContents(snippet);

        // identify all components added
        final Set<String> identifiers = new HashSet<>();
        snippet.getProcessors().stream()
                .map(proc -> proc.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getConnections().stream()
                .map(conn -> conn.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getInputPorts().stream()
                .map(port -> port.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getOutputPorts().stream()
                .map(port -> port.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getProcessGroups().stream()
                .map(group -> group.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getRemoteProcessGroups().stream()
                .map(remoteGroup -> remoteGroup.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getRemoteProcessGroups().stream()
                .filter(remoteGroup -> remoteGroup.getContents() != null && remoteGroup.getContents().getInputPorts() != null)
                .flatMap(remoteGroup -> remoteGroup.getContents().getInputPorts().stream())
                .map(remoteInputPort -> remoteInputPort.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getRemoteProcessGroups().stream()
                .filter(remoteGroup -> remoteGroup.getContents() != null && remoteGroup.getContents().getOutputPorts() != null)
                .flatMap(remoteGroup -> remoteGroup.getContents().getOutputPorts().stream())
                .map(remoteOutputPort -> remoteOutputPort.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getLabels().stream()
                .map(label -> label.getId())
                .forEach(id -> identifiers.add(id));

        return revisionManager.get(identifiers,
                () -> {
                    final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
                    final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
                    return dtoFactory.createFlowDto(group, groupStatus, snippet, revisionManager);
                });
    }

    @Override
    public FlowEntity createTemplateInstance(final String groupId, final Double originX, final Double originY, final String templateId, final String idGenerationSeed) {
        final FlowDTO flowDto = revisionManager.get(groupId, rev -> {
            // instantiate the template - there is no need to make another copy of the flow snippet since the actual template
            // was copied and this dto is only used to instantiate it's components (which as already completed)
            final FlowSnippetDTO snippet = templateDAO.instantiateTemplate(groupId, originX, originY, templateId, idGenerationSeed);

            // save the flow
            controllerFacade.save();

            // post process the new flow snippet
            return postProcessNewFlowSnippet(groupId, snippet);
        });

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    @Override
    public ProcessGroupEntity createArchive() {
        try {
            controllerFacade.createArchive();
        } catch (final IOException e) {
            logger.error("Failed to create an archive", e);
        }

        return getProcessGroup("root");
    }

    @Override
    public ControllerServiceEntity createControllerService(final Revision revision, final String groupId, final ControllerServiceDTO controllerServiceDTO) {
        controllerServiceDTO.setParentGroupId(groupId);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = revisionManager.requestClaim(revision, user);

        final RevisionUpdate<ControllerServiceDTO> snapshot;
        if (groupId == null) {
            try {
                // update revision through revision manager
                snapshot = revisionManager.updateRevision(claim, user, () -> {
                    // Unfortunately, we can not use the createComponent() method here because createComponent() wants to obtain the read lock
                    // on the group. The Controller Service may or may not have a Process Group (it won't if it's controller-scoped).
                    final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);
                    final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

                    controllerFacade.save();

                    final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getUserName());
                    return new StandardRevisionUpdate<ControllerServiceDTO>(dto, lastMod);
                });
            } finally {
                // cancel in case of exception... noop if successful
                revisionManager.cancelClaim(revision.getComponentId());
            }
        } else {
            snapshot = revisionManager.get(groupId, groupRevision -> {
                try {
                    return revisionManager.updateRevision(claim, user, () -> {
                        final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);
                        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

                        controllerFacade.save();

                        final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getUserName());
                        return new StandardRevisionUpdate<ControllerServiceDTO>(dto, lastMod);
                    });
                } finally {
                    // cancel in case of exception... noop if successful
                    revisionManager.cancelClaim(revision.getComponentId());
                }
            });
        }

        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerService);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, bulletins);
    }

    @Override
    public ControllerServiceEntity updateControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final RevisionUpdate<ControllerServiceDTO> snapshot = updateComponent(revision,
                controllerService,
                () -> controllerServiceDAO.updateControllerService(controllerServiceDTO),
                cs -> dtoFactory.createControllerServiceDto(cs));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerService);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, bulletins);
    }

    @Override
    public ControllerServiceReferencingComponentsEntity updateControllerServiceReferencingComponents(
            final Map<String, Revision> referenceRevisions, final String controllerServiceId, final ScheduledState scheduledState, final ControllerServiceState controllerServiceState) {

        final RevisionClaim claim = new StandardRevisionClaim(referenceRevisions.values());

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionUpdate<ControllerServiceReferencingComponentsEntity> update = revisionManager.updateRevision(claim, user,
                new UpdateRevisionTask<ControllerServiceReferencingComponentsEntity>() {
                    @Override
                    public RevisionUpdate<ControllerServiceReferencingComponentsEntity> update() {
                        final Set<ConfiguredComponent> updated = controllerServiceDAO.updateControllerServiceReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
                        final ControllerServiceReference updatedReference = controllerServiceDAO.getControllerService(controllerServiceId).getReferences();

                        final Map<String, Revision> updatedRevisions = new HashMap<>();
                        for (final ConfiguredComponent component : updated) {
                            final Revision currentRevision = revisionManager.getRevision(component.getIdentifier());
                            final Revision requestRevision = referenceRevisions.get(component.getIdentifier());
                            updatedRevisions.put(component.getIdentifier(), currentRevision.incrementRevision(requestRevision.getClientId()));
                        }

                        final ControllerServiceReferencingComponentsEntity entity = createControllerServiceReferencingComponentsEntity(updatedReference, updatedRevisions);
                        return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                    }
                });

        return update.getComponent();
    }

    /**
     * Finds the identifiers for all components referencing a ControllerService.
     *
     * @param reference      ControllerServiceReference
     * @param referencingIds Collection of identifiers
     * @param visited        ControllerServices we've already visited
     */
    private void findControllerServiceReferencingComponentIdentifiers(final ControllerServiceReference reference, final Set<String> referencingIds, final Set<ControllerServiceNode> visited) {
        for (final ConfiguredComponent component : reference.getReferencingComponents()) {
            referencingIds.add(component.getIdentifier());

            // if this is a ControllerService consider it's referencing components
            if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) component;
                if (!visited.contains(node)) {
                    findControllerServiceReferencingComponentIdentifiers(node.getReferences(), referencingIds, visited);
                }
                visited.add(node);
            }
        }
    }

    /**
     * Creates entities for components referencing a ControllerService using their current revision.
     *
     * @param reference ControllerServiceReference
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(final ControllerServiceReference reference, final Set<String> lockedIds) {
        final Set<String> referencingIds = new HashSet<>();
        final Set<ControllerServiceNode> visited = new HashSet<>();
        visited.add(reference.getReferencedComponent());
        findControllerServiceReferencingComponentIdentifiers(reference, referencingIds, visited);

        // TODO remove once we can update a read lock
        referencingIds.removeAll(lockedIds);

        return revisionManager.get(referencingIds, () -> {
            final Map<String, Revision> referencingRevisions = new HashMap<>();
            for (final ConfiguredComponent component : reference.getReferencingComponents()) {
                referencingRevisions.put(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
            }
            return createControllerServiceReferencingComponentsEntity(reference, referencingRevisions);
        });
    }

    /**
     * Creates entities for components referencing a ControllerService using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions) {
        return createControllerServiceReferencingComponentsEntity(reference, revisions, new HashSet<>());
    }

    /**
     * Creates entities for compnents referencing a ControllerServcie using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @param visited   Which services we've already considered (in case of cycle)
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions, final Set<ControllerServiceNode> visited) {

        final String modifier = NiFiUserUtils.getNiFiUserName();
        final Set<ConfiguredComponent> referencingComponents = reference.getReferencingComponents();

        final Set<ControllerServiceReferencingComponentEntity> componentEntities = new HashSet<>();
        for (final ConfiguredComponent refComponent : referencingComponents) {
            AccessPolicyDTO accessPolicy = null;
            if (refComponent instanceof Authorizable) {
                accessPolicy = dtoFactory.createAccessPolicyDto(refComponent);
            }

            final Revision revision = revisions.get(refComponent.getIdentifier());
            final FlowModification flowMod = new FlowModification(revision, modifier);
            final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(flowMod);
            final ControllerServiceReferencingComponentDTO dto = dtoFactory.createControllerServiceReferencingComponentDTO(refComponent);

            if (refComponent instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) refComponent;

                // indicate if we've hit a cycle
                dto.setReferenceCycle(visited.contains(node));

                // if we haven't encountered this service before include it's referencing components
                if (!dto.getReferenceCycle()) {
                    final ControllerServiceReferencingComponentsEntity references = createControllerServiceReferencingComponentsEntity(node.getReferences(), revisions, visited);
                    dto.setReferencingComponents(references.getControllerServiceReferencingComponents());
                }

                // mark node as visited
                visited.add(node);
            }

            componentEntities.add(entityFactory.createControllerServiceReferencingComponentEntity(dto, revisionDto, accessPolicy));
        }

        final ControllerServiceReferencingComponentsEntity entity = new ControllerServiceReferencingComponentsEntity();
        entity.setControllerServiceReferencingComponents(componentEntities);
        return entity;
    }

    @Override
    public ControllerServiceEntity deleteControllerService(final Revision revision, final String controllerServiceId) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        final ControllerServiceDTO snapshot = deleteComponent(
                revision,
                controllerService,
                () -> controllerServiceDAO.deleteControllerService(controllerServiceId),
                dtoFactory.createControllerServiceDto(controllerService));

        return entityFactory.createControllerServiceEntity(snapshot, null, null, null);
    }


    @Override
    public ReportingTaskEntity createReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = revisionManager.requestClaim(revision, user);
        try {
            // update revision through revision manager
            final RevisionUpdate<ReportingTaskDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
                // create the reporting task
                final ReportingTaskNode reportingTask = reportingTaskDAO.createReportingTask(reportingTaskDTO);

                // save the update
                controllerFacade.save();

                final ReportingTaskDTO dto = dtoFactory.createReportingTaskDto(reportingTask);
                final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getUserName());
                return new StandardRevisionUpdate<ReportingTaskDTO>(dto, lastMod);
            });

            final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
            return entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, bulletins);
        } finally {
            // cancel in case of exception... noop if successful
            revisionManager.cancelClaim(revision.getComponentId());
        }
    }

    @Override
    public ReportingTaskEntity updateReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final RevisionUpdate<ReportingTaskDTO> snapshot = updateComponent(revision,
                reportingTask,
                () -> reportingTaskDAO.updateReportingTask(reportingTaskDTO),
                rt -> dtoFactory.createReportingTaskDto(rt));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        return entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, bulletins);
    }

    @Override
    public ReportingTaskEntity deleteReportingTask(final Revision revision, final String reportingTaskId) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        final ReportingTaskDTO snapshot = deleteComponent(
                revision,
                reportingTask,
                () -> reportingTaskDAO.deleteReportingTask(reportingTaskId),
                dtoFactory.createReportingTaskDto(reportingTask));

        return entityFactory.createReportingTaskEntity(snapshot, null, null, null);
    }

    @Override
    public void deleteActions(final Date endDate) {
        // get the user from the request
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the purge details
        final FlowChangePurgeDetails details = new FlowChangePurgeDetails();
        details.setEndDate(endDate);

        // create a purge action to record that records are being removed
        final FlowChangeAction purgeAction = new FlowChangeAction();
        purgeAction.setUserIdentity(user.getIdentity());
        purgeAction.setUserName(user.getUserName());
        purgeAction.setOperation(Operation.Purge);
        purgeAction.setTimestamp(new Date());
        purgeAction.setSourceId("Flow Controller");
        purgeAction.setSourceName("History");
        purgeAction.setSourceType(Component.Controller);
        purgeAction.setActionDetails(details);

        // purge corresponding actions
        auditService.purgeActions(endDate, purgeAction);
    }

    @Override
    public ProvenanceDTO submitProvenance(final ProvenanceDTO query) {
        return controllerFacade.submitProvenance(query);
    }

    @Override
    public void deleteProvenance(final String queryId) {
        controllerFacade.deleteProvenanceQuery(queryId);
    }

    @Override
    public LineageDTO submitLineage(final LineageDTO lineage) {
        return controllerFacade.submitLineage(lineage);
    }

    @Override
    public void deleteLineage(final String lineageId) {
        controllerFacade.deleteLineage(lineageId);
    }

    @Override
    public ProvenanceEventDTO submitReplay(final Long eventId) {
        return controllerFacade.submitReplay(eventId);
    }

    // -----------------------------------------
    // Read Operations
    // -----------------------------------------

    @Override
    public SearchResultsDTO searchController(final String query) {
        return controllerFacade.search(query);
    }

    @Override
    public DownloadableContent getContent(final String connectionId, final String flowFileUuid, final String uri) {
        return connectionDAO.getContent(connectionId, flowFileUuid, uri);
    }

    @Override
    public DownloadableContent getContent(final Long eventId, final String uri, final ContentDirection contentDirection) {
        return controllerFacade.getContent(eventId, uri, contentDirection);
    }

    @Override
    public ProvenanceDTO getProvenance(final String queryId) {
        return controllerFacade.getProvenanceQuery(queryId);
    }

    @Override
    public LineageDTO getLineage(final String lineageId) {
        return controllerFacade.getLineage(lineageId);
    }

    @Override
    public ProvenanceOptionsDTO getProvenanceSearchOptions() {
        return controllerFacade.getProvenanceSearchOptions();
    }

    @Override
    public ProvenanceEventDTO getProvenanceEvent(final Long id) {
        return controllerFacade.getProvenanceEvent(id);
    }

    @Override
    public ProcessGroupStatusDTO getProcessGroupStatus(final String groupId) {
        return dtoFactory.createProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(groupId));
    }

    @Override
    public ControllerStatusDTO getControllerStatus() {
        return controllerFacade.getControllerStatus();
    }

    @Override
    public ComponentStateDTO getProcessorState(final String processorId) {
        return revisionManager.get(processorId, new ReadOnlyRevisionCallback<ComponentStateDTO>() {
            @Override
            public ComponentStateDTO withRevision(final Revision revision) {
                final StateMap clusterState = isClustered() ? processorDAO.getState(processorId, Scope.CLUSTER) : null;
                final StateMap localState = processorDAO.getState(processorId, Scope.LOCAL);

                // processor will be non null as it was already found when getting the state
                final ProcessorNode processor = processorDAO.getProcessor(processorId);
                return dtoFactory.createComponentStateDTO(processorId, processor.getProcessor().getClass(), localState, clusterState);
            }
        });
    }

    @Override
    public ComponentStateDTO getControllerServiceState(final String controllerServiceId) {
        return revisionManager.get(controllerServiceId, new ReadOnlyRevisionCallback<ComponentStateDTO>() {
            @Override
            public ComponentStateDTO withRevision(final Revision revision) {
                final StateMap clusterState = isClustered() ? controllerServiceDAO.getState(controllerServiceId, Scope.CLUSTER) : null;
                final StateMap localState = controllerServiceDAO.getState(controllerServiceId, Scope.LOCAL);

                // controller service will be non null as it was already found when getting the state
                final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
                return dtoFactory.createComponentStateDTO(controllerServiceId, controllerService.getControllerServiceImplementation().getClass(), localState, clusterState);
            }
        });
    }

    @Override
    public ComponentStateDTO getReportingTaskState(final String reportingTaskId) {
        return revisionManager.get(reportingTaskId, new ReadOnlyRevisionCallback<ComponentStateDTO>() {
            @Override
            public ComponentStateDTO withRevision(final Revision revision) {
                final StateMap clusterState = isClustered() ? reportingTaskDAO.getState(reportingTaskId, Scope.CLUSTER) : null;
                final StateMap localState = reportingTaskDAO.getState(reportingTaskId, Scope.LOCAL);

                // reporting task will be non null as it was already found when getting the state
                final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
                return dtoFactory.createComponentStateDTO(reportingTaskId, reportingTask.getReportingTask().getClass(), localState, clusterState);
            }
        });
    }

    @Override
    public CountersDTO getCounters() {
        final List<Counter> counters = controllerFacade.getCounters();
        final Set<CounterDTO> counterDTOs = new LinkedHashSet<>(counters.size());
        for (final Counter counter : counters) {
            counterDTOs.add(dtoFactory.createCounterDto(counter));
        }

        final CountersSnapshotDTO snapshotDto = dtoFactory.createCountersDto(counterDTOs);
        final CountersDTO countersDto = new CountersDTO();
        countersDto.setAggregateSnapshot(snapshotDto);

        return countersDto;
    }

    @Override
    public Set<ConnectionEntity> getConnections(final String groupId) {
        return revisionManager.get(groupId, rev -> {
            final Set<Connection> connections = connectionDAO.getConnections(groupId);
            final Set<String> connectionIds = connections.stream().map(connection -> connection.getIdentifier()).collect(Collectors.toSet());
            return revisionManager.get(connectionIds, () -> {
                return connections.stream()
                        .map(connection -> {
                            final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(connection.getIdentifier()));
                            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
                            final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connection.getIdentifier()));
                            return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connection), revision, accessPolicy, status);
                        })
                        .collect(Collectors.toSet());
            });
        });
    }

    @Override
    public ConnectionEntity getConnection(final String connectionId) {
        return revisionManager.get(connectionId, rev -> {
            final Connection connection = connectionDAO.getConnection(connectionId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
            final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionId));
            return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connectionDAO.getConnection(connectionId)), revision, accessPolicy, status);
        });
    }

    @Override
    public DropRequestDTO getFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.getFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO getFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.getFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public FlowFileDTO getFlowFile(final String connectionId, final String flowFileUuid) {
        return dtoFactory.createFlowFileDTO(connectionDAO.getFlowFile(connectionId, flowFileUuid));
    }

    @Override
    public ConnectionStatusDTO getConnectionStatus(final String connectionId) {
        return revisionManager.get(connectionId, rev -> dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionId)));
    }

    @Override
    public StatusHistoryDTO getConnectionStatusHistory(final String connectionId) {
        return revisionManager.get(connectionId, rev -> controllerFacade.getConnectionStatusHistory(connectionId));
    }

    @Override
    public Set<ProcessorEntity> getProcessors(final String groupId) {
        final Set<ProcessorNode> processors = processorDAO.getProcessors(groupId);
        final Set<String> ids = processors.stream().map(proc -> proc.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return processors.stream()
                    .map(processor -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processor.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processor);
                        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processor.getIdentifier()));
                        final List<BulletinDTO> bulletins =
                                dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processor.getIdentifier()));
                        return entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), revision, accessPolicy, status, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public TemplateDTO exportTemplate(final String id) {
        final Template template = templateDAO.getTemplate(id);
        final TemplateDTO templateDetails = template.getDetails();

        final TemplateDTO templateDTO = dtoFactory.createTemplateDTO(template);
        templateDTO.setSnippet(dtoFactory.copySnippetContents(templateDetails.getSnippet()));
        return templateDTO;
    }

    @Override
    public TemplateDTO getTemplate(final String id) {
        return dtoFactory.createTemplateDTO(templateDAO.getTemplate(id));
    }

    @Override
    public Set<TemplateDTO> getTemplates() {
        final Set<TemplateDTO> templateDtos = new LinkedHashSet<>();
        for (final Template template : templateDAO.getTemplates()) {
            templateDtos.add(dtoFactory.createTemplateDTO(template));
        }
        return templateDtos;
    }

    @Override
    public Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes() {
        return controllerFacade.getFlowFileComparatorTypes();
    }

    @Override
    public Set<DocumentedTypeDTO> getProcessorTypes() {
        return controllerFacade.getFlowFileProcessorTypes();
    }

    @Override
    public Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType) {
        return controllerFacade.getControllerServiceTypes(serviceType);
    }

    @Override
    public Set<DocumentedTypeDTO> getReportingTaskTypes() {
        return controllerFacade.getReportingTaskTypes();
    }

    @Override
    public ProcessorEntity getProcessor(final String id) {
        return revisionManager.get(id, rev -> {
            final ProcessorNode processor = processorDAO.getProcessor(id);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(id));
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(id));
            return entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), revision, dtoFactory.createAccessPolicyDto(processor), status, bulletins);
        });
    }

    @Override
    public PropertyDescriptorDTO getProcessorPropertyDescriptor(final String id, final String property) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        PropertyDescriptor descriptor = processor.getPropertyDescriptor(property);

        // return an invalid descriptor if the processor doesn't suppor this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, processor.getProcessGroup().getIdentifier());
    }

    @Override
    public ProcessorStatusDTO getProcessorStatus(final String id) {
        return revisionManager.get(id, rev -> dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(id)));
    }

    @Override
    public StatusHistoryDTO getProcessorStatusHistory(final String id) {
        return controllerFacade.getProcessorStatusHistory(id);
    }

    @Override
    public BulletinBoardDTO getBulletinBoard(final BulletinQueryDTO query) {
        // build the query
        final BulletinQuery.Builder queryBuilder = new BulletinQuery.Builder()
                .groupIdMatches(query.getGroupId())
                .sourceIdMatches(query.getSourceId())
                .nameMatches(query.getName())
                .messageMatches(query.getMessage())
                .after(query.getAfter())
                .limit(query.getLimit());

        // perform the query
        final List<Bulletin> results = bulletinRepository.findBulletins(queryBuilder.build());

        // perform the query and generate the results - iterating in reverse order since we are
        // getting the most recent results by ordering by timestamp desc above. this gets the
        // exact results we want but in reverse order
        final List<BulletinDTO> bulletins = new ArrayList<>();
        for (final ListIterator<Bulletin> bulletinIter = results.listIterator(results.size()); bulletinIter.hasPrevious(); ) {
            bulletins.add(dtoFactory.createBulletinDto(bulletinIter.previous()));
        }

        // create the bulletin board
        final BulletinBoardDTO bulletinBoard = new BulletinBoardDTO();
        bulletinBoard.setBulletins(bulletins);
        bulletinBoard.setGenerated(new Date());
        return bulletinBoard;
    }

    @Override
    public SystemDiagnosticsDTO getSystemDiagnostics() {
        final SystemDiagnostics sysDiagnostics = controllerFacade.getSystemDiagnostics();
        return dtoFactory.createSystemDiagnosticsDto(sysDiagnostics);
    }

    @Override
    public List<ResourceDTO> getResources() {
        final List<Resource> resources = controllerFacade.getResources();
        final List<ResourceDTO> resourceDtos = new ArrayList<>(resources.size());
        for (final Resource resource : resources) {
            resourceDtos.add(dtoFactory.createResourceDto(resource));
        }
        return resourceDtos;
    }

    /**
     * Ensures the specified user has permission to access the specified port.
     */
    private boolean isUserAuthorized(final NiFiUser user, final RootGroupPort port) {
        final boolean isSiteToSiteSecure = Boolean.TRUE.equals(properties.isSiteToSiteSecure());

        // if site to site is not secure, allow all users
        if (!isSiteToSiteSecure) {
            return true;
        }

        // TODO - defer to authorizer to see if user is able to retrieve site-to-site details for the specified port
        return true;
    }

    @Override
    public ControllerDTO getController() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // TODO - defer to authorizer to see if user is able to retrieve site-to-site details

        // TODO - filter response for access to specific ports

        // serialize the input ports this NiFi has access to
        final Set<PortDTO> inputPortDtos = new LinkedHashSet<>();
        final Set<RootGroupPort> inputPorts = controllerFacade.getInputPorts();
        final Set<String> inputPortIds = inputPorts.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());
        revisionManager.get(inputPortIds, () -> {
            for (final RootGroupPort inputPort : inputPorts) {
                if (isUserAuthorized(user, inputPort)) {
                    final PortDTO dto = new PortDTO();
                    dto.setId(inputPort.getIdentifier());
                    dto.setName(inputPort.getName());
                    dto.setComments(inputPort.getComments());
                    dto.setState(inputPort.getScheduledState().toString());
                    inputPortDtos.add(dto);
                }
            }
            return null;
        });

        // serialize the output ports this NiFi has access to
        final Set<PortDTO> outputPortDtos = new LinkedHashSet<>();
        final Set<RootGroupPort> outputPorts = controllerFacade.getOutputPorts();
        final Set<String> outputPortIds = outputPorts.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());
        revisionManager.get(outputPortIds, () -> {
            for (final RootGroupPort outputPort : controllerFacade.getOutputPorts()) {
                if (isUserAuthorized(user, outputPort)) {
                    final PortDTO dto = new PortDTO();
                    dto.setId(outputPort.getIdentifier());
                    dto.setName(outputPort.getName());
                    dto.setComments(outputPort.getComments());
                    dto.setState(outputPort.getScheduledState().toString());
                    outputPortDtos.add(dto);
                }
            }

            return null;
        });

        // get the root group
        final String rootGroupId = controllerFacade.getRootGroupId();
        final ProcessGroupCounts counts = revisionManager.get(rootGroupId, rev -> {
            final ProcessGroup rootGroup = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
            return rootGroup.getCounts();
        });

        // create the controller dto
        final ControllerDTO controllerDTO = new ControllerDTO();
        controllerDTO.setId(controllerFacade.getRootGroupId());
        controllerDTO.setInstanceId(controllerFacade.getInstanceId());
        controllerDTO.setInputPorts(inputPortDtos);
        controllerDTO.setOutputPorts(outputPortDtos);
        controllerDTO.setInputPortCount(inputPorts.size());
        controllerDTO.setOutputPortCount(outputPortDtos.size());
        controllerDTO.setRunningCount(counts.getRunningCount());
        controllerDTO.setStoppedCount(counts.getStoppedCount());
        controllerDTO.setInvalidCount(counts.getInvalidCount());
        controllerDTO.setDisabledCount(counts.getDisabledCount());

        // determine the site to site configuration
        if (isClustered()) {
            controllerDTO.setRemoteSiteListeningPort(controllerFacade.getClusterManagerRemoteSiteListeningPort());
            controllerDTO.setRemoteSiteHttpListeningPort(controllerFacade.getClusterManagerRemoteSiteListeningHttpPort());
            controllerDTO.setSiteToSiteSecure(controllerFacade.isClusterManagerRemoteSiteCommsSecure());
        } else {
            controllerDTO.setRemoteSiteListeningPort(controllerFacade.getRemoteSiteListeningPort());
            controllerDTO.setRemoteSiteHttpListeningPort(controllerFacade.getRemoteSiteListeningHttpPort());
            controllerDTO.setSiteToSiteSecure(controllerFacade.isRemoteSiteCommsSecure());
        }

        return controllerDTO;
    }

    @Override
    public ControllerConfigurationEntity getControllerConfiguration() {
        return revisionManager.get(FlowController.class.getSimpleName(), rev -> {
            final ControllerConfigurationDTO dto = dtoFactory.createControllerConfigurationDto(controllerFacade);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerFacade);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            return entityFactory.createControllerConfigurationEntity(dto, revision, accessPolicy);
        });
    }

    @Override
    public FlowConfigurationEntity getFlowConfiguration() {
        final FlowConfigurationDTO dto = dtoFactory.createFlowConfigurationDto(properties.getAutoRefreshInterval());
        final FlowConfigurationEntity entity = new FlowConfigurationEntity();
        entity.setFlowConfiguration(dto);
        return entity;
    }

    @Override
    public AccessPolicyEntity getAccessPolicy(final String accessPolicyId) {
        AccessPolicy preRevisionRequestAccessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
        Set<String> ids = Stream.concat(Stream.of(accessPolicyId),
                Stream.concat(preRevisionRequestAccessPolicy.getUsers().stream(), preRevisionRequestAccessPolicy.getGroups().stream())).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            final RevisionDTO requestedAccessPolicyRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(accessPolicyId));
            final AccessPolicy requestedAccessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(authorizableLookup.getAccessPolicyAuthorizable(accessPolicyId));
            return entityFactory.createAccessPolicyEntity(
                    dtoFactory.createAccessPolicyDto(requestedAccessPolicy,
                            requestedAccessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()),
                            requestedAccessPolicy.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet())),
                    requestedAccessPolicyRevision, accessPolicy);
        });
    }

    @Override
    public UserEntity getUser(final String userId) {
        final Authorizable usersAuthorizable = authorizableLookup.getTenantAuthorizable();
        Set<String> ids = Stream.concat(Stream.of(userId), userDAO.getUser(userId).getGroups().stream()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(usersAuthorizable);
            final User user = userDAO.getUser(userId);
            final Set<TenantEntity> userGroups = user.getGroups().stream()
                    .map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
            return entityFactory.createUserEntity(dtoFactory.createUserDto(user, userGroups), userRevision, accessPolicy);
        });
    }

    @Override
    public Set<UserEntity> getUsers() {
        final Set<User> users = userDAO.getUsers();
        final Set<String> ids = users.stream().flatMap(user -> Stream.concat(Stream.of(user.getIdentifier()), user.getGroups().stream())).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return users.stream()
                    .map(user -> {
                        final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(user.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(authorizableLookup.getTenantAuthorizable());
                        final Set<TenantEntity> userGroups = user.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
                        return entityFactory.createUserEntity(dtoFactory.createUserDto(user, userGroups), userRevision, accessPolicy);
                    }).collect(Collectors.toSet());
        });
    }

    @Override
    public UserGroupEntity getUserGroup(final String userGroupId) {
        Set<String> ids = Stream.concat(Stream.of(userGroupId), userGroupDAO.getUserGroup(userGroupId).getUsers().stream()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroupId));
            final Group userGroup = userGroupDAO.getUserGroup(userGroupId);
            final Set<TenantEntity> users = userGroup.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
            return entityFactory.createUserGroupEntity(dtoFactory.createUserGroupDto(userGroup, users), userGroupRevision,
                    dtoFactory.createAccessPolicyDto(authorizableLookup.getTenantAuthorizable()));
        });
    }

    @Override
    public Set<UserGroupEntity> getUserGroups() {
        final Authorizable userGroupAuthorizable = authorizableLookup.getTenantAuthorizable();
        final Set<Group> userGroups = userGroupDAO.getUserGroups();
        final Set<String> ids = userGroups.stream().flatMap(userGroup -> Stream.concat(Stream.of(userGroup.getIdentifier()), userGroup.getUsers().stream())).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return userGroups.stream()
                    .map(userGroup -> {
                        final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroup.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(userGroupAuthorizable);
                        final Set<TenantEntity> users = userGroup.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
                        return entityFactory.createUserGroupEntity(dtoFactory.createUserGroupDto(userGroup, users), userGroupRevision, accessPolicy);
                    }).collect(Collectors.toSet());
        });
    }

    @Override
    public Set<LabelEntity> getLabels(final String groupId) {
        final Set<Label> labels = labelDAO.getLabels(groupId);
        final Set<String> ids = labels.stream().map(label -> label.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return labels.stream()
                    .map(label -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(label.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
                        return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), revision, accessPolicy);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public LabelEntity getLabel(final String labelId) {
        return revisionManager.get(labelId, rev -> {
            final Label label = labelDAO.getLabel(labelId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
            return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), revision, accessPolicy);
        });
    }

    @Override
    public Set<FunnelEntity> getFunnels(final String groupId) {
        final Set<Funnel> funnels = funnelDAO.getFunnels(groupId);
        final Set<String> funnelIds = funnels.stream().map(funnel -> funnel.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(funnelIds, () -> {
            return funnels.stream()
                    .map(funnel -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(funnel.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
                        return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), revision, accessPolicy);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public FunnelEntity getFunnel(final String funnelId) {
        return revisionManager.get(funnelId, rev -> {
            final Funnel funnel = funnelDAO.getFunnel(funnelId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
            return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), revision, accessPolicy);
        });
    }

    @Override
    public Set<PortEntity> getInputPorts(final String groupId) {
        final Set<Port> inputPorts = inputPortDAO.getPorts(groupId);
        final Set<String> portIds = inputPorts.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(portIds, () -> {
            return inputPorts.stream()
                    .map(port -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
                        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
                        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
                        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public Set<PortEntity> getOutputPorts(final String groupId) {
        final Set<Port> ports = outputPortDAO.getPorts(groupId);
        final Set<String> ids = ports.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());

        return revisionManager.get(ids, () -> {
            return ports.stream()
                    .map(port -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
                        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
                        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
                        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public Set<ProcessGroupEntity> getProcessGroups(final String parentGroupId) {
        final Set<ProcessGroup> groups = processGroupDAO.getProcessGroups(parentGroupId);
        final Set<String> ids = groups.stream().map(group -> group.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return groups.stream()
                    .map(group -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(group.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(group);
                        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(group.getIdentifier()));
                        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(group.getIdentifier()));
                        return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(group), revision, accessPolicy, status, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public Set<RemoteProcessGroupEntity> getRemoteProcessGroups(final String groupId) {
        final Set<RemoteProcessGroup> rpgs = remoteProcessGroupDAO.getRemoteProcessGroups(groupId);
        final Set<String> ids = rpgs.stream().map(rpg -> rpg.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return rpgs.stream()
                    .map(rpg -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(rpg.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(rpg);
                        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(rpg.getIdentifier()));
                        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(rpg.getIdentifier()));
                        return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), revision, accessPolicy, status, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public PortEntity getInputPort(final String inputPortId) {
        return revisionManager.get(inputPortId, rev -> {
            final Port port = inputPortDAO.getPort(inputPortId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
            final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortId));
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(inputPortId));
            return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status, bulletins);
        });
    }

    @Override
    public PortStatusDTO getInputPortStatus(final String inputPortId) {
        return revisionManager.get(inputPortId, rev -> dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortId)));
    }

    @Override
    public PortEntity getOutputPort(final String outputPortId) {
        return revisionManager.get(outputPortId, rev -> {
            final Port port = outputPortDAO.getPort(outputPortId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
            final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortId));
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(outputPortId));
            return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status, bulletins);
        });
    }

    @Override
    public PortStatusDTO getOutputPortStatus(final String outputPortId) {
        return revisionManager.get(outputPortId, rev -> dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortId)));
    }

    @Override
    public RemoteProcessGroupEntity getRemoteProcessGroup(final String remoteProcessGroupId) {
        return revisionManager.get(remoteProcessGroupId, rev -> {
            final RemoteProcessGroup rpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(rpg);
            final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(rpg.getIdentifier()));
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(rpg.getIdentifier()));
            return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), revision, accessPolicy, status, bulletins);
        });
    }

    @Override
    public RemoteProcessGroupStatusDTO getRemoteProcessGroupStatus(final String id) {
        return revisionManager.get(id, rev -> dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(id)));
    }

    @Override
    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(final String id) {
        return controllerFacade.getRemoteProcessGroupStatusHistory(id);
    }

    @Override
    public ProcessGroupFlowEntity getProcessGroupFlow(final String groupId, final boolean recurse) {
        return revisionManager.get(groupId,
                rev -> {
                    // get all identifiers for every child component
                    final Set<String> identifiers = new HashSet<>();
                    final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
                    processGroup.getProcessors().stream()
                            .map(proc -> proc.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getConnections().stream()
                            .map(conn -> conn.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getInputPorts().stream()
                            .map(port -> port.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getOutputPorts().stream()
                            .map(port -> port.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getProcessGroups().stream()
                            .map(group -> group.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getRemoteProcessGroups().stream()
                            .map(remoteGroup -> remoteGroup.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getRemoteProcessGroups().stream()
                            .flatMap(remoteGroup -> remoteGroup.getInputPorts().stream())
                            .map(remoteInputPort -> remoteInputPort.getIdentifier())
                            .forEach(id -> identifiers.add(id));
                    processGroup.getRemoteProcessGroups().stream()
                            .flatMap(remoteGroup -> remoteGroup.getOutputPorts().stream())
                            .map(remoteOutputPort -> remoteOutputPort.getIdentifier())
                            .forEach(id -> identifiers.add(id));

                    // read lock on every component being accessed in the dto conversion
                    return revisionManager.get(identifiers,
                            () -> {
                                final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
                                final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
                                return entityFactory.createProcessGroupFlowEntity(dtoFactory.createProcessGroupFlowDto(processGroup, groupStatus, revisionManager), accessPolicy);
                            });
                });
    }

    @Override
    public ProcessGroupEntity getProcessGroup(final String groupId) {
        return revisionManager.get(groupId, rev -> {
            final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
            final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(groupId));
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(groupId));
            return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(processGroup), revision, accessPolicy, status, bulletins);
        });
    }

    @Override
    public Set<ControllerServiceEntity> getControllerServices(final String groupId) {
        final Set<ControllerServiceNode> serviceNodes = controllerServiceDAO.getControllerServices(groupId);
        final Set<String> serviceIds = serviceNodes.stream().map(service -> service.getIdentifier()).collect(Collectors.toSet());

        return revisionManager.get(serviceIds, () -> {
            return serviceNodes.stream()
                    .map(serviceNode -> {
                        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);

                        final ControllerServiceReference ref = serviceNode.getReferences();
                        final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref, serviceIds);
                        dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());

                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(serviceNode.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(serviceNode);
                        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(serviceNode.getIdentifier()));
                        return entityFactory.createControllerServiceEntity(dto, revision, accessPolicy, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public ControllerServiceEntity getControllerService(final String controllerServiceId) {
        return revisionManager.get(controllerServiceId, rev -> {
            final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerService);
            final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

            final ControllerServiceReference ref = controllerService.getReferences();
            final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref, Sets.newHashSet(controllerServiceId));
            dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());

            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceId));

            return entityFactory.createControllerServiceEntity(dto, revision, accessPolicy, bulletins);
        });
    }

    @Override
    public PropertyDescriptorDTO getControllerServicePropertyDescriptor(final String id, final String property) {
        return revisionManager.get(id, rev -> {
            final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
            PropertyDescriptor descriptor = controllerService.getControllerServiceImplementation().getPropertyDescriptor(property);

            // return an invalid descriptor if the controller service doesn't support this property
            if (descriptor == null) {
                descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
            }

            final String groupId = controllerService.getProcessGroup() == null ? null : controllerService.getProcessGroup().getIdentifier();
            return dtoFactory.createPropertyDescriptorDto(descriptor, groupId);
        });
    }

    @Override
    public ControllerServiceReferencingComponentsEntity getControllerServiceReferencingComponents(final String controllerServiceId) {
        return revisionManager.get(controllerServiceId, rev -> {
            final ControllerServiceNode service = controllerServiceDAO.getControllerService(controllerServiceId);
            final ControllerServiceReference ref = service.getReferences();
            return createControllerServiceReferencingComponentsEntity(ref, Sets.newHashSet(controllerServiceId));
        });
    }

    @Override
    public Set<ReportingTaskEntity> getReportingTasks() {
        final Set<ReportingTaskNode> reportingTasks = reportingTaskDAO.getReportingTasks();
        final Set<String> ids = reportingTasks.stream().map(task -> task.getIdentifier()).collect(Collectors.toSet());

        return revisionManager.get(ids, () -> {
            return reportingTasks.stream()
                    .map(reportingTask -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(reportingTask.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
                        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
                        return entityFactory.createReportingTaskEntity(dtoFactory.createReportingTaskDto(reportingTask), revision, accessPolicy, bulletins);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public ReportingTaskEntity getReportingTask(final String reportingTaskId) {
        return revisionManager.get(reportingTaskId, rev -> {
            final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTaskId));
            return entityFactory.createReportingTaskEntity(dtoFactory.createReportingTaskDto(reportingTask), revision, accessPolicy, bulletins);
        });
    }

    @Override
    public PropertyDescriptorDTO getReportingTaskPropertyDescriptor(final String id, final String property) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(id);
        PropertyDescriptor descriptor = reportingTask.getReportingTask().getPropertyDescriptor(property);

        // return an invalid descriptor if the reporting task doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, "root");
    }

    @Override
    public StatusHistoryDTO getProcessGroupStatusHistory(final String groupId) {
        return controllerFacade.getProcessGroupStatusHistory(groupId);
    }

    private boolean authorizeAction(final Action action) {
        final String sourceId = action.getSourceId();
        final Component type = action.getSourceType();

        final Authorizable authorizable;
        try {
            switch (type) {
                case Processor:
                    authorizable = authorizableLookup.getProcessor(sourceId);
                    break;
                case ReportingTask:
                    authorizable = authorizableLookup.getReportingTask(sourceId);
                    break;
                case ControllerService:
                    authorizable = authorizableLookup.getControllerService(sourceId);
                    break;
                case Controller:
                    authorizable = controllerFacade;
                    break;
                case InputPort:
                    authorizable = authorizableLookup.getInputPort(sourceId);
                    break;
                case OutputPort:
                    authorizable = authorizableLookup.getOutputPort(sourceId);
                    break;
                case ProcessGroup:
                    authorizable = authorizableLookup.getProcessGroup(sourceId);
                    break;
                case RemoteProcessGroup:
                    authorizable = authorizableLookup.getRemoteProcessGroup(sourceId);
                    break;
                case Funnel:
                    authorizable = authorizableLookup.getFunnel(sourceId);
                    break;
                case Connection:
                    authorizable = authorizableLookup.getConnection(sourceId);
                    break;
                default:
                    throw new WebApplicationException(Response.serverError().entity("An unexpected type of component is the source of this action.").build());
            }
        } catch (final ResourceNotFoundException e) {
            // if the underlying component is gone, disallow
            return false;
        }

        // perform the authorization
        final AuthorizationResult result = authorizable.checkAuthorization(authorizer, RequestAction.READ);
        return Result.Approved.equals(result.getResult());
    }

    @Override
    public HistoryDTO getActions(final HistoryQueryDTO historyQueryDto) {
        // extract the query criteria
        final HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setStartDate(historyQueryDto.getStartDate());
        historyQuery.setEndDate(historyQueryDto.getEndDate());
        historyQuery.setSourceId(historyQueryDto.getSourceId());
        historyQuery.setUserName(historyQueryDto.getUserName());
        historyQuery.setOffset(historyQueryDto.getOffset());
        historyQuery.setCount(historyQueryDto.getCount());
        historyQuery.setSortColumn(historyQueryDto.getSortColumn());
        historyQuery.setSortOrder(historyQueryDto.getSortOrder());

        // perform the query
        final History history = auditService.getActions(historyQuery);

        // only retain authorized actions
        history.getActions().stream().filter(action -> authorizeAction(action)).collect(Collectors.toList());

        // create the response
        return dtoFactory.createHistoryDto(history);
    }

    @Override
    public ActionDTO getAction(final Integer actionId) {
        // get the action
        final Action action = auditService.getAction(actionId);

        // ensure the action was found
        if (action == null) {
            throw new ResourceNotFoundException(String.format("Unable to find action with id '%s'.", actionId));
        }

        if (!authorizeAction(action)) {
            throw new AccessDeniedException("Access is denied.");
        }

        // return the action
        return dtoFactory.createActionDto(action);
    }

    @Override
    public ComponentHistoryDTO getComponentHistory(final String componentId) {
        final Map<String, PropertyHistoryDTO> propertyHistoryDtos = new LinkedHashMap<>();
        final Map<String, List<PreviousValue>> propertyHistory = auditService.getPreviousValues(componentId);

        for (final Map.Entry<String, List<PreviousValue>> entry : propertyHistory.entrySet()) {
            final List<PreviousValueDTO> previousValueDtos = new ArrayList<>();

            for (final PreviousValue previousValue : entry.getValue()) {
                final PreviousValueDTO dto = new PreviousValueDTO();
                dto.setPreviousValue(previousValue.getPreviousValue());
                dto.setTimestamp(previousValue.getTimestamp());
                dto.setUserName(previousValue.getUserName());
                previousValueDtos.add(dto);
            }

            if (!previousValueDtos.isEmpty()) {
                final PropertyHistoryDTO propertyHistoryDto = new PropertyHistoryDTO();
                propertyHistoryDto.setPreviousValues(previousValueDtos);
                propertyHistoryDtos.put(entry.getKey(), propertyHistoryDto);
            }
        }

        final ComponentHistoryDTO history = new ComponentHistoryDTO();
        history.setComponentId(componentId);
        history.setPropertyHistory(propertyHistoryDtos);

        return history;
    }

    @Override
    public boolean isClustered() {
        return controllerFacade.isClustered();
    }

    @Override
    public String getNodeId() {
        final NodeIdentifier nodeId = controllerFacade.getNodeId();
        if (nodeId != null) {
            return nodeId.getId();
        } else {
            return null;
        }
    }

    @Override
    public ClusterDTO getCluster() {
        // create cluster summary dto
        final ClusterDTO clusterDto = new ClusterDTO();

        // set current time
        clusterDto.setGenerated(new Date());

        // create node dtos
        final Collection<NodeDTO> nodeDtos = new ArrayList<>();
        clusterDto.setNodes(nodeDtos);
        final NodeIdentifier primaryNode = clusterCoordinator.getPrimaryNode();
        for (final NodeIdentifier nodeId : clusterCoordinator.getNodeIdentifiers()) {
            final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(nodeId);
            if (status == null) {
                continue;
            }

            final List<NodeEvent> events = clusterCoordinator.getNodeEvents(nodeId);
            final boolean primary = primaryNode != null && primaryNode.equals(nodeId);
            final NodeHeartbeat heartbeat = heartbeatMonitor.getLatestHeartbeat(nodeId);
            nodeDtos.add(dtoFactory.createNodeDTO(nodeId, status, heartbeat, events, primary));
        }

        return clusterDto;
    }

    @Override
    public NodeDTO getNode(final String nodeId) {
        final NodeIdentifier nodeIdentifier = clusterCoordinator.getNodeIdentifier(nodeId);
        return getNode(nodeIdentifier);
    }

    private NodeDTO getNode(final NodeIdentifier nodeId) {
        final NodeConnectionStatus nodeStatus = clusterCoordinator.getConnectionStatus(nodeId);
        final List<NodeEvent> events = clusterCoordinator.getNodeEvents(nodeId);
        final boolean primary = nodeId.equals(clusterCoordinator.getPrimaryNode());
        final NodeHeartbeat heartbeat = heartbeatMonitor.getLatestHeartbeat(nodeId);
        return dtoFactory.createNodeDTO(nodeId, nodeStatus, heartbeat, events, primary);
    }

    @Override
    public void deleteNode(final String nodeId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final String userDn = user.getIdentity();
        final NodeIdentifier nodeIdentifier = clusterCoordinator.getNodeIdentifier(nodeId);
        if (nodeIdentifier == null) {
            throw new UnknownNodeException("Cannot remove Node with ID " + nodeId + " because it is not part of the cluster");
        }

        clusterCoordinator.removeNode(nodeIdentifier, userDn);
        heartbeatMonitor.removeHeartbeat(nodeIdentifier);
    }

    /* reusable function declarations for converting ids to tenant entities */
    private Function<String, TenantEntity> mapUserGroupIdToTenantEntity() {
        return userGroupId -> {
            final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroupId));
            return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userGroupDAO.getUserGroup(userGroupId)), userGroupRevision,
                    dtoFactory.createAccessPolicyDto(authorizableLookup.getTenantAuthorizable()));
        };
    }

    private Function<String, TenantEntity> mapUserIdToTenantEntity() {
        return userId -> {
            final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
            return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userDAO.getUser(userId)), userRevision,
                    dtoFactory.createAccessPolicyDto(authorizableLookup.getTenantAuthorizable()));
        };
    }


    /* setters */
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    public void setControllerFacade(final ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }

    public void setRemoteProcessGroupDAO(final RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    public void setLabelDAO(final LabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    public void setFunnelDAO(final FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    public void setSnippetDAO(final SnippetDAO snippetDAO) {
        this.snippetDAO = snippetDAO;
    }

    public void setProcessorDAO(final ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    public void setConnectionDAO(final ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setEntityFactory(final EntityFactory entityFactory) {
        this.entityFactory = entityFactory;
    }

    public void setInputPortDAO(final PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    public void setOutputPortDAO(final PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    public void setProcessGroupDAO(final ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    public void setControllerServiceDAO(final ControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }

    public void setReportingTaskDAO(final ReportingTaskDAO reportingTaskDAO) {
        this.reportingTaskDAO = reportingTaskDAO;
    }

    public void setTemplateDAO(final TemplateDAO templateDAO) {
        this.templateDAO = templateDAO;
    }

    public void setSnippetUtils(final SnippetUtils snippetUtils) {
        this.snippetUtils = snippetUtils;
    }

    public void setAuthorizableLookup(final AuthorizableLookup authorizableLookup) {
        this.authorizableLookup = authorizableLookup;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setUserDAO(final UserDAO userDAO) {
        this.userDAO = userDAO;
    }

    public void setUserGroupDAO(final UserGroupDAO userGroupDAO) {
        this.userGroupDAO = userGroupDAO;
    }

    public void setAccessPolicyDAO(final AccessPolicyDAO accessPolicyDAO) {
        this.accessPolicyDAO = accessPolicyDAO;
    }

    public void setClusterCoordinator(final ClusterCoordinator coordinator) {
        this.clusterCoordinator = coordinator;
    }

    public void setHeartbeatMonitor(final HeartbeatMonitor heartbeatMonitor) {
        this.heartbeatMonitor = heartbeatMonitor;
    }

    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }
}
