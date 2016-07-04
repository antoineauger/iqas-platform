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
package org.apache.nifi.web.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

/**
 * Template utilities.
 */
public final class SnippetUtils {

    private FlowController flowController;
    private DtoFactory dtoFactory;

    /**
     * Populates the specified snippet and returns the details.
     *
     * @param snippet snippet
     * @param recurse recurse
     * @param includeControllerServices whether or not to include controller services in the flow snippet dto
     * @return snippet
     */
    public FlowSnippetDTO populateFlowSnippet(final Snippet snippet, final boolean recurse, final boolean includeControllerServices) {
        final FlowSnippetDTO snippetDto = new FlowSnippetDTO();
        final String groupId = snippet.getParentGroupId();
        final ProcessGroup processGroup = flowController.getGroup(groupId);

        // ensure the group could be found
        if (processGroup == null) {
            throw new IllegalStateException("The parent process group for this snippet could not be found.");
        }

        final Set<ControllerServiceDTO> controllerServices = new HashSet<>();

        // add any processors
        if (!snippet.getProcessors().isEmpty()) {
            final Set<ProcessorDTO> processors = new LinkedHashSet<>();
            for (final String processorId : snippet.getProcessors().keySet()) {
                final ProcessorNode processor = processGroup.getProcessor(processorId);
                if (processor == null) {
                    throw new IllegalStateException("A processor in this snippet could not be found.");
                }
                processors.add(dtoFactory.createProcessorDto(processor));

                if (includeControllerServices) {
                    controllerServices.addAll(getControllerServices(processor.getProperties()));
                }
            }
            snippetDto.setProcessors(processors);
        }

        // add any connections
        if (!snippet.getConnections().isEmpty()) {
            final Set<ConnectionDTO> connections = new LinkedHashSet<>();
            for (final String connectionId : snippet.getConnections().keySet()) {
                final Connection connection = processGroup.getConnection(connectionId);
                if (connection == null) {
                    throw new IllegalStateException("A connection in this snippet could not be found.");
                }
                connections.add(dtoFactory.createConnectionDto(connection));
            }
            snippetDto.setConnections(connections);
        }

        // add any funnels
        if (!snippet.getFunnels().isEmpty()) {
            final Set<FunnelDTO> funnels = new LinkedHashSet<>();
            for (final String funnelId : snippet.getFunnels().keySet()) {
                final Funnel funnel = processGroup.getFunnel(funnelId);
                if (funnel == null) {
                    throw new IllegalStateException("A funnel in this snippet could not be found.");
                }
                funnels.add(dtoFactory.createFunnelDto(funnel));
            }
            snippetDto.setFunnels(funnels);
        }

        // add any input ports
        if (!snippet.getInputPorts().isEmpty()) {
            final Set<PortDTO> inputPorts = new LinkedHashSet<>();
            for (final String inputPortId : snippet.getInputPorts().keySet()) {
                final Port inputPort = processGroup.getInputPort(inputPortId);
                if (inputPort == null) {
                    throw new IllegalStateException("An input port in this snippet could not be found.");
                }
                inputPorts.add(dtoFactory.createPortDto(inputPort));
            }
            snippetDto.setInputPorts(inputPorts);
        }

        // add any labels
        if (!snippet.getLabels().isEmpty()) {
            final Set<LabelDTO> labels = new LinkedHashSet<>();
            for (final String labelId : snippet.getLabels().keySet()) {
                final Label label = processGroup.getLabel(labelId);
                if (label == null) {
                    throw new IllegalStateException("A label in this snippet could not be found.");
                }
                labels.add(dtoFactory.createLabelDto(label));
            }
            snippetDto.setLabels(labels);
        }

        // add any output ports
        if (!snippet.getOutputPorts().isEmpty()) {
            final Set<PortDTO> outputPorts = new LinkedHashSet<>();
            for (final String outputPortId : snippet.getOutputPorts().keySet()) {
                final Port outputPort = processGroup.getOutputPort(outputPortId);
                if (outputPort == null) {
                    throw new IllegalStateException("An output port in this snippet could not be found.");
                }
                outputPorts.add(dtoFactory.createPortDto(outputPort));
            }
            snippetDto.setOutputPorts(outputPorts);
        }

        // add any process groups
        if (!snippet.getProcessGroups().isEmpty()) {
            final Set<ProcessGroupDTO> processGroups = new LinkedHashSet<>();
            for (final String childGroupId : snippet.getProcessGroups().keySet()) {
                final ProcessGroup childGroup = processGroup.getProcessGroup(childGroupId);
                if (childGroup == null) {
                    throw new IllegalStateException("A process group in this snippet could not be found.");
                }

                final ProcessGroupDTO childGroupDto = dtoFactory.createProcessGroupDto(childGroup, recurse);
                processGroups.add(childGroupDto);

                addControllerServices(childGroup, childGroupDto);
            }
            snippetDto.setProcessGroups(processGroups);
        }

        // add any remote process groups
        if (!snippet.getRemoteProcessGroups().isEmpty()) {
            final Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
            for (final String remoteProcessGroupId : snippet.getRemoteProcessGroups().keySet()) {
                final RemoteProcessGroup remoteProcessGroup = processGroup.getRemoteProcessGroup(remoteProcessGroupId);
                if (remoteProcessGroup == null) {
                    throw new IllegalStateException("A remote process group in this snippet could not be found.");
                }
                remoteProcessGroups.add(dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));
            }
            snippetDto.setRemoteProcessGroups(remoteProcessGroups);
        }

        snippetDto.setControllerServices(controllerServices);

        return snippetDto;
    }

    private void addControllerServices(final ProcessGroup group, final ProcessGroupDTO dto) {
        final FlowSnippetDTO contents = dto.getContents();
        if (contents == null) {
            return;
        }

        final Set<ControllerServiceDTO> controllerServices = new HashSet<>();

        for (final ProcessorNode procNode : group.getProcessors()) {
            final Set<ControllerServiceDTO> servicesForProcessor = getControllerServices(procNode.getProperties());
            controllerServices.addAll(servicesForProcessor);
        }

        contents.setControllerServices(controllerServices);

        // Map child process group ID to the child process group for easy lookup
        final Map<String, ProcessGroupDTO> childGroupMap = contents.getProcessGroups().stream()
            .collect(Collectors.toMap(childGroupDto -> childGroupDto.getId(), childGroupDto -> childGroupDto));

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final ProcessGroupDTO childDto = childGroupMap.get(childGroup.getIdentifier());
            if (childDto == null) {
                continue;
            }

            addControllerServices(childGroup, childDto);
        }
    }

    private Set<ControllerServiceDTO> getControllerServices(final Map<PropertyDescriptor, String> componentProperties) {
        final Set<ControllerServiceDTO> serviceDtos = new HashSet<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : componentProperties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String controllerServiceId = entry.getValue();
                if (controllerServiceId != null) {
                    final ControllerServiceNode serviceNode = flowController.getControllerServiceNode(controllerServiceId);
                    if (serviceNode != null) {
                        serviceDtos.add(dtoFactory.createControllerServiceDto(serviceNode));

                        final Set<ControllerServiceDTO> recursiveRefs = getControllerServices(serviceNode.getProperties());
                        serviceDtos.addAll(recursiveRefs);
                    }
                }
            }
        }

        return serviceDtos;
    }


    public FlowSnippetDTO copy(final FlowSnippetDTO snippetContents, final ProcessGroup group, final String idGenerationSeed) {
        final FlowSnippetDTO snippetCopy = copyContentsForGroup(snippetContents, group.getIdentifier(), null, null, idGenerationSeed);
        resolveNameConflicts(snippetCopy, group);
        return snippetCopy;
    }

    private void resolveNameConflicts(final FlowSnippetDTO snippetContents, final ProcessGroup group) {
        // get a list of all names of ports so that we can rename the ports as needed.
        final List<String> existingPortNames = new ArrayList<>();
        for (final Port inputPort : group.getInputPorts()) {
            existingPortNames.add(inputPort.getName());
        }
        for (final Port outputPort : group.getOutputPorts()) {
            existingPortNames.add(outputPort.getName());
        }

        // rename ports
        if (snippetContents.getInputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getInputPorts()) {
                String portName = portDTO.getName();
                while (existingPortNames.contains(portName)) {
                    portName = "Copy of " + portName;
                }
                portDTO.setName(portName);
                existingPortNames.add(portDTO.getName());
            }
        }
        if (snippetContents.getOutputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getOutputPorts()) {
                String portName = portDTO.getName();
                while (existingPortNames.contains(portName)) {
                    portName = "Copy of " + portName;
                }
                portDTO.setName(portName);
                existingPortNames.add(portDTO.getName());
            }
        }

        // get a list of all names of process groups so that we can rename as needed.
        final List<String> groupNames = new ArrayList<>();
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            groupNames.add(childGroup.getName());
        }

        if (snippetContents.getProcessGroups() != null) {
            for (final ProcessGroupDTO groupDTO : snippetContents.getProcessGroups()) {
                String groupName = groupDTO.getName();
                while (groupNames.contains(groupName)) {
                    groupName = "Copy of " + groupName;
                }
                groupDTO.setName(groupName);
                groupNames.add(groupDTO.getName());
            }
        }
    }

    private FlowSnippetDTO copyContentsForGroup(final FlowSnippetDTO snippetContents, final String groupId, final Map<String, ConnectableDTO> parentConnectableMap, Map<String, String> serviceIdMap,
        final String idGenerationSeed) {
        final FlowSnippetDTO snippetContentsCopy = new FlowSnippetDTO();

        //
        // Copy the Controller Services
        //
        if (serviceIdMap == null) {
            serviceIdMap = new HashMap<>();
        }

        final Set<ControllerServiceDTO> services = new HashSet<>();
        if (snippetContents.getControllerServices() != null) {
            for (final ControllerServiceDTO serviceDTO : snippetContents.getControllerServices()) {
                final ControllerServiceDTO service = dtoFactory.copy(serviceDTO);
                service.setId(generateId(serviceDTO.getId(), idGenerationSeed));
                service.setState(ControllerServiceState.DISABLED.name());
                services.add(service);

                // Map old service ID to new service ID so that we can make sure that we reference the new ones.
                serviceIdMap.put(serviceDTO.getId(), service.getId());
            }
        }

        // if there is any controller service that maps to another controller service, update the id's
        for (final ControllerServiceDTO serviceDTO : services) {
            final Map<String, String> properties = serviceDTO.getProperties();
            final Map<String, PropertyDescriptorDTO> descriptors = serviceDTO.getDescriptors();
            if (properties != null && descriptors != null) {
                for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                    if (descriptor.getIdentifiesControllerService() != null) {
                        final String currentServiceId = properties.get(descriptor.getName());
                        if (currentServiceId == null) {
                            continue;
                        }

                        final String newServiceId = serviceIdMap.get(currentServiceId);
                        properties.put(descriptor.getName(), newServiceId);
                    }
                }
            }
        }
        snippetContentsCopy.setControllerServices(services);

        //
        // Copy the labels
        //
        final Set<LabelDTO> labels = new HashSet<>();
        if (snippetContents.getLabels() != null) {
            for (final LabelDTO labelDTO : snippetContents.getLabels()) {
                final LabelDTO label = dtoFactory.copy(labelDTO);
                label.setId(generateId(labelDTO.getId(), idGenerationSeed));
                label.setParentGroupId(groupId);
                labels.add(label);
            }
        }
        snippetContentsCopy.setLabels(labels);

        //
        // Copy connectable components
        //
        // maps a group ID-ID of a Connectable in the template to the new instance
        final Map<String, ConnectableDTO> connectableMap = new HashMap<>();

        //
        // Copy the funnels
        //
        final Set<FunnelDTO> funnels = new HashSet<>();
        if (snippetContents.getFunnels() != null) {
            for (final FunnelDTO funnelDTO : snippetContents.getFunnels()) {
                final FunnelDTO cp = dtoFactory.copy(funnelDTO);
                cp.setId(generateId(funnelDTO.getId(), idGenerationSeed));
                cp.setParentGroupId(groupId);
                funnels.add(cp);

                connectableMap.put(funnelDTO.getParentGroupId() + "-" + funnelDTO.getId(), dtoFactory.createConnectableDto(cp));
            }
        }
        snippetContentsCopy.setFunnels(funnels);

        final Set<PortDTO> inputPorts = new HashSet<>();
        if (snippetContents.getInputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getInputPorts()) {
                final PortDTO cp = dtoFactory.copy(portDTO);
                cp.setId(generateId(portDTO.getId(), idGenerationSeed));
                cp.setParentGroupId(groupId);
                cp.setState(ScheduledState.STOPPED.toString());
                inputPorts.add(cp);

                final ConnectableDTO portConnectable = dtoFactory.createConnectableDto(cp, ConnectableType.INPUT_PORT);
                connectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                if (parentConnectableMap != null) {
                    parentConnectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                }
            }
        }
        snippetContentsCopy.setInputPorts(inputPorts);

        final Set<PortDTO> outputPorts = new HashSet<>();
        if (snippetContents.getOutputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getOutputPorts()) {
                final PortDTO cp = dtoFactory.copy(portDTO);
                cp.setId(generateId(portDTO.getId(), idGenerationSeed));
                cp.setParentGroupId(groupId);
                cp.setState(ScheduledState.STOPPED.toString());
                outputPorts.add(cp);

                final ConnectableDTO portConnectable = dtoFactory.createConnectableDto(cp, ConnectableType.OUTPUT_PORT);
                connectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                if (parentConnectableMap != null) {
                    parentConnectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                }
            }
        }
        snippetContentsCopy.setOutputPorts(outputPorts);

        //
        // Copy the processors
        //
        final Set<ProcessorDTO> processors = new HashSet<>();
        if (snippetContents.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : snippetContents.getProcessors()) {
                final ProcessorDTO cp = dtoFactory.copy(processorDTO);
                cp.setId(generateId(processorDTO.getId(), idGenerationSeed));
                cp.setParentGroupId(groupId);
                cp.setState(ScheduledState.STOPPED.toString());
                processors.add(cp);

                connectableMap.put(processorDTO.getParentGroupId() + "-" + processorDTO.getId(), dtoFactory.createConnectableDto(cp));
            }
        }
        snippetContentsCopy.setProcessors(processors);

        // if there is any controller service that maps to another controller service, update the id's
        updateControllerServiceIdentifiers(snippetContentsCopy, serviceIdMap);

        //
        // Copy ProcessGroups
        //
        // instantiate the process groups, renaming as necessary
        final Set<ProcessGroupDTO> groups = new HashSet<>();
        if (snippetContents.getProcessGroups() != null) {
            for (final ProcessGroupDTO groupDTO : snippetContents.getProcessGroups()) {
                final ProcessGroupDTO cp = dtoFactory.copy(groupDTO, false);
                cp.setId(generateId(groupDTO.getId(), idGenerationSeed));
                cp.setParentGroupId(groupId);

                // copy the contents of this group - we do not copy via the dto factory since we want to specify new ids
                final FlowSnippetDTO contentsCopy = copyContentsForGroup(groupDTO.getContents(), cp.getId(), connectableMap, serviceIdMap, idGenerationSeed);
                cp.setContents(contentsCopy);
                groups.add(cp);
            }
        }
        snippetContentsCopy.setProcessGroups(groups);

        final Set<RemoteProcessGroupDTO> remoteGroups = new HashSet<>();
        if (snippetContents.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteGroupDTO : snippetContents.getRemoteProcessGroups()) {
                final RemoteProcessGroupDTO cp = dtoFactory.copy(remoteGroupDTO);
                cp.setId(generateId(remoteGroupDTO.getId(), idGenerationSeed));
                cp.setParentGroupId(groupId);

                final RemoteProcessGroupContentsDTO contents = cp.getContents();
                if (contents != null && contents.getInputPorts() != null) {
                    for (final RemoteProcessGroupPortDTO remotePort : contents.getInputPorts()) {
                        remotePort.setGroupId(cp.getId());
                        connectableMap.put(remoteGroupDTO.getId() + "-" + remotePort.getId(), dtoFactory.createConnectableDto(remotePort, ConnectableType.REMOTE_INPUT_PORT));
                    }
                }
                if (contents != null && contents.getOutputPorts() != null) {
                    for (final RemoteProcessGroupPortDTO remotePort : contents.getOutputPorts()) {
                        remotePort.setGroupId(cp.getId());
                        connectableMap.put(remoteGroupDTO.getId() + "-" + remotePort.getId(), dtoFactory.createConnectableDto(remotePort, ConnectableType.REMOTE_OUTPUT_PORT));
                    }
                }

                remoteGroups.add(cp);
            }
        }
        snippetContentsCopy.setRemoteProcessGroups(remoteGroups);

        final Set<ConnectionDTO> connections = new HashSet<>();
        if (snippetContents.getConnections() != null) {
            for (final ConnectionDTO connectionDTO : snippetContents.getConnections()) {
                final ConnectionDTO cp = dtoFactory.copy(connectionDTO);

                final ConnectableDTO source = connectableMap.get(cp.getSource().getGroupId() + "-" + cp.getSource().getId());
                final ConnectableDTO destination = connectableMap.get(cp.getDestination().getGroupId() + "-" + cp.getDestination().getId());

                // ensure all referenced components are present
                if (source == null || destination == null) {
                    throw new IllegalArgumentException("The flow snippet contains a Connection that references a component that is not included.");
                }

                cp.setId(generateId(connectionDTO.getId(), idGenerationSeed));
                cp.setSource(source);
                cp.setDestination(destination);
                cp.setParentGroupId(groupId);
                connections.add(cp);
            }
        }
        snippetContentsCopy.setConnections(connections);

        return snippetContentsCopy;
    }

    private void updateControllerServiceIdentifiers(final FlowSnippetDTO snippet, final Map<String, String> serviceIdMap) {
        final Set<ProcessorDTO> processors = snippet.getProcessors();
        if (processors != null) {
            for (final ProcessorDTO processor : processors) {
                updateControllerServiceIdentifiers(processor.getConfig(), serviceIdMap);
            }
        }

        for (final ProcessGroupDTO processGroupDto : snippet.getProcessGroups()) {
            updateControllerServiceIdentifiers(processGroupDto.getContents(), serviceIdMap);
        }
    }

    private void updateControllerServiceIdentifiers(final ProcessorConfigDTO configDto, final Map<String, String> serviceIdMap) {
        if (configDto == null) {
            return;
        }

        final Map<String, String> properties = configDto.getProperties();
        final Map<String, PropertyDescriptorDTO> descriptors = configDto.getDescriptors();
        if (properties != null && descriptors != null) {
            for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                if (descriptor.getIdentifiesControllerService() != null) {
                    final String currentServiceId = properties.get(descriptor.getName());
                    if (currentServiceId == null) {
                        continue;
                    }

                    // if this is a copy/paste action, we can continue to reference the same service, in this case
                    // the serviceIdMap will be empty
                    if (serviceIdMap.containsKey(currentServiceId)) {
                        final String newServiceId = serviceIdMap.get(currentServiceId);
                        properties.put(descriptor.getName(), newServiceId);
                    }
                }
            }
        }
    }

    /**
     * Generates a new id for the current id that is specified. If no seed is found, a new random id will be created.
     */
    private String generateId(final String currentId, final String seed) {
        if (seed == null) {
            return UUID.randomUUID().toString();
        } else {
            return UUID.nameUUIDFromBytes((currentId + seed).getBytes(StandardCharsets.UTF_8)).toString();
        }
    }

    /* setters */
    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

}
