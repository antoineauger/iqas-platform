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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import static org.apache.nifi.util.StringUtils.isEmpty;

public class StandardRemoteProcessGroupDAO extends ComponentDAO implements RemoteProcessGroupDAO {

    private static final Logger logger = LoggerFactory.getLogger(StandardRemoteProcessGroupDAO.class);
    private FlowController flowController;

    private RemoteProcessGroup locateRemoteProcessGroup(final String remoteProcessGroupId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        final RemoteProcessGroup remoteProcessGroup = rootGroup.findRemoteProcessGroup(remoteProcessGroupId);

        if (remoteProcessGroup == null) {
            throw new ResourceNotFoundException(String.format("Unable to find remote process group with id '%s'.", remoteProcessGroupId));
        } else {
            return remoteProcessGroup;
        }
    }

    @Override
    public boolean hasRemoteProcessGroup(String remoteProcessGroupId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        return rootGroup.findRemoteProcessGroup(remoteProcessGroupId) != null;
    }

    /**
     * Creates a remote process group reference.
     *
     * @param remoteProcessGroupDTO The remote process group
     * @return The remote process group
     */
    @Override
    public RemoteProcessGroup createRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        if (remoteProcessGroupDTO.getParentGroupId() != null && !flowController.areGroupsSame(groupId, remoteProcessGroupDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Remote Process Group is being added.");
        }

        final String rawTargetUri = remoteProcessGroupDTO.getTargetUri();
        if (rawTargetUri == null) {
            throw new IllegalArgumentException("Cannot add a Remote Process Group without specifying the Target URI");
        }

        // create the remote process group
        RemoteProcessGroup remoteProcessGroup = flowController.createRemoteProcessGroup(remoteProcessGroupDTO.getId(), rawTargetUri);

        // set other properties
        updateRemoteProcessGroup(remoteProcessGroup, remoteProcessGroupDTO);

        // get the group to add the remote process group to
        group.addRemoteProcessGroup(remoteProcessGroup);

        return remoteProcessGroup;
    }

    /**
     * Gets the specified remote process group.
     *
     * @param remoteProcessGroupId The remote process group id
     * @return The remote process group
     */
    @Override
    public RemoteProcessGroup getRemoteProcessGroup(String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);

        return remoteProcessGroup;
    }

    /**
     * Gets all of the remote process groups.
     *
     * @return The remote process groups
     */
    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups(String groupId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final Set<RemoteProcessGroup> remoteProcessGroups = group.getRemoteProcessGroups();
        return remoteProcessGroups;
    }

    @Override
    public void verifyUpdate(RemoteProcessGroupDTO remoteProcessGroup) {
        verifyUpdate(locateRemoteProcessGroup(remoteProcessGroup.getId()), remoteProcessGroup);
    }

    /**
     * Verifies the specified remote group can be updated, if necessary.
     */
    private void verifyUpdate(RemoteProcessGroup remoteProcessGroup, RemoteProcessGroupDTO remoteProcessGroupDto) {
        // see if the remote process group can start/stop transmitting
        if (isNotNull(remoteProcessGroupDto.isTransmitting())) {
            if (!remoteProcessGroup.isTransmitting() && remoteProcessGroupDto.isTransmitting()) {
                remoteProcessGroup.verifyCanStartTransmitting();
            } else if (remoteProcessGroup.isTransmitting() && !remoteProcessGroupDto.isTransmitting()) {
                remoteProcessGroup.verifyCanStopTransmitting();
            }
        }

        // validate the proposed configuration
        final List<String> requestValidation = validateProposedRemoteProcessGroupConfiguration(remoteProcessGroupDto);
        // ensure there was no validation errors
        if (!requestValidation.isEmpty()) {
            throw new ValidationException(requestValidation);
        }

        // if any remote group properties are changing, verify update
        if (isAnyNotNull(remoteProcessGroupDto.getYieldDuration(),
                remoteProcessGroupDto.getCommunicationsTimeout(),
                remoteProcessGroupDto.getProxyHost(),
                remoteProcessGroupDto.getProxyPort(),
                remoteProcessGroupDto.getProxyUser(),
                remoteProcessGroupDto.getProxyPassword())) {
            remoteProcessGroup.verifyCanUpdate();
        }
    }

    @Override
    public void verifyUpdateInputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getInputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group input port with id '%s'.", remoteProcessGroupPortDto.getId()));
        }

        verifyUpdatePort(port, remoteProcessGroupPortDto);
    }

    @Override
    public void verifyUpdateOutputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getOutputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group output port with id '%s'.", remoteProcessGroupPortDto.getId()));
        }

        verifyUpdatePort(port, remoteProcessGroupPortDto);
    }

    /**
     * Verified the specified remote port can be updated, if necessary.
     */
    private void verifyUpdatePort(RemoteGroupPort port, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        // see if the remote process group can start/stop transmitting
        if (isNotNull(remoteProcessGroupPortDto.isTransmitting())) {
            if (!port.isRunning() && remoteProcessGroupPortDto.isTransmitting()) {
                port.verifyCanStart();
            } else if (port.isRunning() && !remoteProcessGroupPortDto.isTransmitting()) {
                port.verifyCanStop();
            }
        }

        // validate the proposed configuration
        final List<String> requestValidation = validateProposedRemoteProcessGroupPortConfiguration(port, remoteProcessGroupPortDto);
        // ensure there was no validation errors
        if (!requestValidation.isEmpty()) {
            throw new ValidationException(requestValidation);
        }


        // verify update when appropriate
        if (isAnyNotNull(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount(), remoteProcessGroupPortDto.getUseCompression())) {
            port.verifyCanUpdate();
        }
    }

    /**
     * Validates the proposed configuration for the specified remote port.
     */
    private List<String> validateProposedRemoteProcessGroupPortConfiguration(RemoteGroupPort remoteGroupPort, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        final List<String> validationErrors = new ArrayList<>();

        // ensure the proposed port configuration is valid
        if (isNotNull(remoteProcessGroupPortDTO.getConcurrentlySchedulableTaskCount()) && remoteProcessGroupPortDTO.getConcurrentlySchedulableTaskCount() <= 0) {
            validationErrors.add(String.format("Concurrent tasks for port '%s' must be a positive integer.", remoteGroupPort.getName()));
        }

        return validationErrors;
    }

    /**
     * Validates the proposed configuration for the specified remote group.
     */
    private List<String> validateProposedRemoteProcessGroupConfiguration(RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final List<String> validationErrors = new ArrayList<>();

        if (isNotNull(remoteProcessGroupDTO.getCommunicationsTimeout())) {
            Matcher yieldMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(remoteProcessGroupDTO.getCommunicationsTimeout());
            if (!yieldMatcher.matches()) {
                validationErrors.add("Communications timeout is not a valid time duration (ie 30 sec, 5 min)");
            }
        }
        if (isNotNull(remoteProcessGroupDTO.getYieldDuration())) {
            Matcher yieldMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(remoteProcessGroupDTO.getYieldDuration());
            if (!yieldMatcher.matches()) {
                validationErrors.add("Yield duration is not a valid time duration (ie 30 sec, 5 min)");
            }
        }
        String proxyPassword = remoteProcessGroupDTO.getProxyPassword();
        String proxyUser = remoteProcessGroupDTO.getProxyUser();
        String proxyHost = remoteProcessGroupDTO.getProxyHost();

        if (isNotNull(remoteProcessGroupDTO.getProxyPort())) {
            if (isEmpty(proxyHost)) {
                validationErrors.add("Proxy port was specified, but proxy host was empty.");
            }
        }

        if (!isEmpty(proxyUser)) {
            if (isEmpty(proxyHost)) {
                validationErrors.add("Proxy user name was specified, but proxy host was empty.");
            }
            if (isEmpty(proxyPassword)) {
                validationErrors.add("User password should be specified if Proxy server needs user authentication.");
            }
        }

        if (!isEmpty(proxyPassword)) {
            if (isEmpty(proxyHost)) {
                validationErrors.add("Proxy user password was specified, but proxy host was empty.");
            }
            if (isEmpty(proxyPassword)) {
                validationErrors.add("User name should be specified if Proxy server needs user authentication.");
            }
        }
        return validationErrors;
    }

    @Override
    public RemoteGroupPort updateRemoteProcessGroupInputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getInputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group input port with id '%s'.", remoteProcessGroupPortDto.getId()));
        }

        // verify the update
        verifyUpdatePort(port, remoteProcessGroupPortDto);

        // perform the update
        if (isNotNull(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount())) {
            port.setMaxConcurrentTasks(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount());
        }
        if (isNotNull(remoteProcessGroupPortDto.getUseCompression())) {
            port.setUseCompression(remoteProcessGroupPortDto.getUseCompression());
        }

        final Boolean isTransmitting = remoteProcessGroupPortDto.isTransmitting();
        if (isNotNull(isTransmitting)) {
            // start or stop as necessary
            if (!port.isRunning() && isTransmitting) {
                remoteProcessGroup.startTransmitting(port);
            } else if (port.isRunning() && !isTransmitting) {
                remoteProcessGroup.stopTransmitting(port);
            }
        }

        return port;
    }

    @Override
    public RemoteGroupPort updateRemoteProcessGroupOutputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getOutputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group output port with id '%s'.", remoteProcessGroupId));
        }

        // verify the update
        verifyUpdatePort(port, remoteProcessGroupPortDto);

        // perform the update
        if (isNotNull(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount())) {
            port.setMaxConcurrentTasks(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount());
        }
        if (isNotNull(remoteProcessGroupPortDto.getUseCompression())) {
            port.setUseCompression(remoteProcessGroupPortDto.getUseCompression());
        }

        final Boolean isTransmitting = remoteProcessGroupPortDto.isTransmitting();
        if (isNotNull(isTransmitting)) {
            // start or stop as necessary
            if (!port.isRunning() && isTransmitting) {
                remoteProcessGroup.startTransmitting(port);
            } else if (port.isRunning() && !isTransmitting) {
                remoteProcessGroup.stopTransmitting(port);
            }
        }

        return port;
    }

    @Override
    public RemoteProcessGroup updateRemoteProcessGroup(RemoteProcessGroupDTO remoteProcessGroupDTO) {
        RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupDTO.getId());
        return updateRemoteProcessGroup(remoteProcessGroup, remoteProcessGroupDTO);


    }

    private RemoteProcessGroup updateRemoteProcessGroup(RemoteProcessGroup remoteProcessGroup, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // verify the update request
        verifyUpdate(remoteProcessGroup, remoteProcessGroupDTO);

        // configure the remote process group
        final String name = remoteProcessGroupDTO.getName();
        final String comments = remoteProcessGroupDTO.getComments();
        final String communicationsTimeout = remoteProcessGroupDTO.getCommunicationsTimeout();
        final String yieldDuration = remoteProcessGroupDTO.getYieldDuration();
        final String proxyHost = remoteProcessGroupDTO.getProxyHost();
        final Integer proxyPort = remoteProcessGroupDTO.getProxyPort();
        final String proxyUser = remoteProcessGroupDTO.getProxyUser();
        final String proxyPassword = remoteProcessGroupDTO.getProxyPassword();

        final String transportProtocol = remoteProcessGroupDTO.getTransportProtocol();

        if (isNotNull(name)) {
            remoteProcessGroup.setName(name);
        }
        if (isNotNull(comments)) {
            remoteProcessGroup.setComments(comments);
        }
        if (isNotNull(communicationsTimeout)) {
            remoteProcessGroup.setCommunicationsTimeout(communicationsTimeout);
        }
        if (isNotNull(yieldDuration)) {
            remoteProcessGroup.setYieldDuration(yieldDuration);
        }
        if (isNotNull(remoteProcessGroupDTO.getPosition())) {
            remoteProcessGroup.setPosition(new Position(remoteProcessGroupDTO.getPosition().getX(), remoteProcessGroupDTO.getPosition().getY()));
        }
        if (isNotNull(transportProtocol)) {
            remoteProcessGroup.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(transportProtocol.toUpperCase()));
            // No null check because these proxy settings have to be clear if not specified.
            // But when user Enable/Disable transmission, only isTransmitting is sent.
            // To prevent clearing these values in that case, set these only if transportProtocol is sent,
            // assuming UI sends transportProtocol always for update.
            remoteProcessGroup.setProxyHost(proxyHost);
            remoteProcessGroup.setProxyPort(proxyPort);
            remoteProcessGroup.setProxyUser(proxyUser);
            // Keep using current password when null or "********" was sent.
            // Passing other values updates the password,
            // specify empty String to clear password.
            if (isNotNull(proxyPassword) && !DtoFactory.SENSITIVE_VALUE_MASK.equals(proxyPassword)) {
                remoteProcessGroup.setProxyPassword(proxyPassword);
            }
        }

        final Boolean isTransmitting = remoteProcessGroupDTO.isTransmitting();
        if (isNotNull(isTransmitting)) {
            // start or stop as necessary
            if (!remoteProcessGroup.isTransmitting() && isTransmitting) {
                remoteProcessGroup.startTransmitting();
            } else if (remoteProcessGroup.isTransmitting() && !isTransmitting) {
                remoteProcessGroup.stopTransmitting();
            }
        }

        return remoteProcessGroup;
    }

    @Override
    public void verifyDelete(String remoteProcessGroupId) {
        RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);
        remoteProcessGroup.verifyCanDelete();
    }

    @Override
    public void deleteRemoteProcessGroup(String remoteProcessGroupId) {
        RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(remoteProcessGroupId);
        remoteProcessGroup.getProcessGroup().removeRemoteProcessGroup(remoteProcessGroup);
    }

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
