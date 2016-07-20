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
package org.apache.nifi.web.api.dto;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * The contents of a flow snippet.
 */
@XmlType(name = "flowSnippet")
public class FlowSnippetDTO {

    private Set<ProcessGroupDTO> processGroups = new LinkedHashSet<>();
    private Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
    private Set<ProcessorDTO> processors = new LinkedHashSet<>();
    private Set<PortDTO> inputPorts = new LinkedHashSet<>();
    private Set<PortDTO> outputPorts = new LinkedHashSet<>();
    private Set<ConnectionDTO> connections = new LinkedHashSet<>();
    private Set<LabelDTO> labels = new LinkedHashSet<>();
    private Set<FunnelDTO> funnels = new LinkedHashSet<>();
    private Set<ControllerServiceDTO> controllerServices = new LinkedHashSet<>();

    private final boolean newTemplate;

    public FlowSnippetDTO() {
        this(false);
    }

    public FlowSnippetDTO(boolean newTemplate) {
        this.newTemplate = newTemplate;
    }
    /**
     * @return connections in this flow snippet
     */
    @ApiModelProperty(
            value = "The connections in this flow snippet."
    )
    public Set<ConnectionDTO> getConnections() {
        return connections;
    }

    public void setConnections(Set<ConnectionDTO> connections) {
        this.removeInstanceIdentifierIfNecessary(connections);
        this.connections = this.orderedById(connections);
    }

    /**
     * @return input ports in this flow snippet
     */
    @ApiModelProperty(
            value = "The input ports in this flow snippet."
    )
    public Set<PortDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<PortDTO> inputPorts) {
        this.removeInstanceIdentifierIfNecessary(inputPorts);
        this.inputPorts = this.orderedById(inputPorts);
    }

    /**
     * @return labels in this flow snippet
     */
    @ApiModelProperty(
            value = "The labels in this flow snippet."
    )
    public Set<LabelDTO> getLabels() {
        return labels;
    }

    public void setLabels(Set<LabelDTO> labels) {
        this.removeInstanceIdentifierIfNecessary(labels);
        this.labels = this.orderedById(labels);
    }

    /**
     * @return funnels in this flow snippet
     */
    @ApiModelProperty(
            value = "The funnels in this flow snippet."
    )
    public Set<FunnelDTO> getFunnels() {
        return funnels;
    }

    public void setFunnels(Set<FunnelDTO> funnels) {
        this.removeInstanceIdentifierIfNecessary(funnels);
        this.funnels = this.orderedById(funnels);
    }

    /**
     * @return output ports in this flow snippet
     */
    @ApiModelProperty(
            value = "The output ports in this flow snippet."
    )
    public Set<PortDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<PortDTO> outputPorts) {
        this.removeInstanceIdentifierIfNecessary(outputPorts);
        this.outputPorts = this.orderedById(outputPorts);
    }

    /**
     * @return process groups in this flow snippet
     */
    @ApiModelProperty(
            value = "The process groups in this flow snippet."
    )
    public Set<ProcessGroupDTO> getProcessGroups() {
        return processGroups;
    }

    public void setProcessGroups(Set<ProcessGroupDTO> processGroups) {
        this.removeInstanceIdentifierIfNecessary(processGroups);
        this.processGroups = this.orderedById(processGroups);
    }

    /**
     * @return processors in this flow group
     */
    @ApiModelProperty(
            value = "The processors in this flow snippet."
    )
    public Set<ProcessorDTO> getProcessors() {
        return processors;
    }

    public void setProcessors(Set<ProcessorDTO> processors) {
        this.removeInstanceIdentifierIfNecessary(processors);
        this.processors = this.orderedById(processors);
    }

    /**
     * @return remote process groups in this flow snippet
     */
    @ApiModelProperty(
            value = "The remote process groups in this flow snippet."
    )
    public Set<RemoteProcessGroupDTO> getRemoteProcessGroups() {
        return remoteProcessGroups;
    }

    public void setRemoteProcessGroups(Set<RemoteProcessGroupDTO> remoteProcessGroups) {
        this.removeInstanceIdentifierIfNecessary(remoteProcessGroups);
        this.remoteProcessGroups = this.orderedById(remoteProcessGroups);
    }

    /**
     * @return the Controller Services in this flow snippet
     */
    @ApiModelProperty(
            value = "The controller services in this flow snippet."
    )
    public Set<ControllerServiceDTO> getControllerServices() {
        return controllerServices;
    }

    public void setControllerServices(Set<ControllerServiceDTO> controllerServices) {
        this.removeInstanceIdentifierIfNecessary(controllerServices);
        this.controllerServices = this.orderedById(controllerServices);
    }

    private <T extends ComponentDTO> Set<T> orderedById(Set<T> dtos) {
        TreeSet<T> components = new TreeSet<>(new Comparator<ComponentDTO>() {
            @Override
            public int compare(ComponentDTO c1, ComponentDTO c2) {
                return UUID.fromString(c1.getId()).compareTo(UUID.fromString(c2.getId()));
            }
        });
        components.addAll(dtos);
        return components;
    }

    private void removeInstanceIdentifierIfNecessary(Set<? extends ComponentDTO> componentDtos) {
        if (this.newTemplate) {
            for (ComponentDTO componentDto : componentDtos) {
                UUID id = UUID.fromString(componentDto.getId());
                id = new UUID(id.getMostSignificantBits(), 0);
                componentDto.setId(id.toString());
                if (componentDto instanceof ConnectionDTO) {
                    ConnectionDTO connectionDTO = (ConnectionDTO) componentDto;
                    ConnectableDTO cdto = connectionDTO.getSource();
                    id = UUID.fromString(cdto.getId());
                    id = new UUID(id.getMostSignificantBits(), 0);
                    cdto.setId(id.toString());

                    cdto = connectionDTO.getDestination();
                    id = UUID.fromString(cdto.getId());
                    id = new UUID(id.getMostSignificantBits(), 0);
                    cdto.setId(id.toString());
                }
            }
        }
    }
}
