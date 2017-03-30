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

package org.apache.nifi.controller.service.mock;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockProcessGroup implements ProcessGroup {
    private final Map<String, ControllerServiceNode> serviceMap = new HashMap<>();
    private final Map<String, ProcessorNode> processorMap = new HashMap<>();

    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return null;
    }

    @Override
    public ProcessGroup getParent() {
        return null;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public void setParent(final ProcessGroup group) {

    }

    @Override
    public String getIdentifier() {
        return "unit test group id";
    }

    @Override
    public String getName() {
        return "unit test group";
    }

    @Override
    public void setName(final String name) {

    }

    @Override
    public void setPosition(final Position position) {

    }

    @Override
    public Position getPosition() {
        return null;
    }

    @Override
    public String getComments() {
        return null;
    }

    @Override
    public void setComments(final String comments) {

    }

    @Override
    public ProcessGroupCounts getCounts() {
        return null;
    }

    @Override
    public void startProcessing() {

    }

    @Override
    public void stopProcessing() {

    }

    @Override
    public void enableProcessor(final ProcessorNode processor) {

    }

    @Override
    public void enableInputPort(final Port port) {

    }

    @Override
    public void enableOutputPort(final Port port) {

    }

    @Override
    public void startProcessor(final ProcessorNode processor) {

    }

    @Override
    public void startInputPort(final Port port) {

    }

    @Override
    public void startOutputPort(final Port port) {

    }

    @Override
    public void startFunnel(final Funnel funnel) {

    }

    @Override
    public void stopProcessor(final ProcessorNode processor) {

    }

    @Override
    public void stopInputPort(final Port port) {

    }

    @Override
    public void stopOutputPort(final Port port) {

    }

    @Override
    public void disableProcessor(final ProcessorNode processor) {

    }

    @Override
    public void disableInputPort(final Port port) {

    }

    @Override
    public void disableOutputPort(final Port port) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isRootGroup() {
        return false;
    }

    @Override
    public void addInputPort(final Port port) {

    }

    @Override
    public void removeInputPort(final Port port) {

    }

    @Override
    public Set<Port> getInputPorts() {
        return null;
    }

    @Override
    public Port getInputPort(final String id) {
        return null;
    }

    @Override
    public void addOutputPort(final Port port) {

    }

    @Override
    public void removeOutputPort(final Port port) {

    }

    @Override
    public Port getOutputPort(final String id) {
        return null;
    }

    @Override
    public Set<Port> getOutputPorts() {
        return null;
    }

    @Override
    public void addProcessGroup(final ProcessGroup group) {

    }

    @Override
    public ProcessGroup getProcessGroup(final String id) {
        return null;
    }

    @Override
    public Set<ProcessGroup> getProcessGroups() {
        return null;
    }

    @Override
    public void removeProcessGroup(final ProcessGroup group) {

    }

    @Override
    public void addProcessor(final ProcessorNode processor) {
        processor.setProcessGroup(this);
        processorMap.put(processor.getIdentifier(), processor);
    }

    @Override
    public void removeProcessor(final ProcessorNode processor) {
        processorMap.remove(processor.getIdentifier());
    }

    @Override
    public Set<ProcessorNode> getProcessors() {
        return new HashSet<>(processorMap.values());
    }

    @Override
    public ProcessorNode getProcessor(final String id) {
        return processorMap.get(id);
    }

    @Override
    public Set<Positionable> findAllPositionables() {
        return null;
    }

    @Override
    public Connectable getConnectable(final String id) {
        return null;
    }

    @Override
    public void addConnection(final Connection connection) {

    }

    @Override
    public void removeConnection(final Connection connection) {

    }

    @Override
    public void inheritConnection(final Connection connection) {

    }

    @Override
    public Connection getConnection(final String id) {
        return null;
    }

    @Override
    public Set<Connection> getConnections() {
        return null;
    }

    @Override
    public Connection findConnection(final String id) {
        return null;
    }

    @Override
    public List<Connection> findAllConnections() {
        return null;
    }

    @Override
    public Funnel findFunnel(final String id) {
        return null;
    }

    @Override
    public ControllerServiceNode findControllerService(final String id) {
        return serviceMap.get(id);
    }

    @Override
    public Set<ControllerServiceNode> findAllControllerServices() {
        return new HashSet<>(serviceMap.values());
    }

    @Override
    public void addRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {

    }

    @Override
    public void removeRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {

    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup(final String id) {
        return null;
    }

    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups() {
        return null;
    }

    @Override
    public void addLabel(final Label label) {

    }

    @Override
    public void removeLabel(final Label label) {

    }

    @Override
    public Set<Label> getLabels() {
        return null;
    }

    @Override
    public Label getLabel(final String id) {
        return null;
    }

    @Override
    public ProcessGroup findProcessGroup(final String id) {
        return null;
    }

    @Override
    public List<ProcessGroup> findAllProcessGroups() {
        return null;
    }

    @Override
    public RemoteProcessGroup findRemoteProcessGroup(final String id) {
        return null;
    }

    @Override
    public List<RemoteProcessGroup> findAllRemoteProcessGroups() {
        return null;
    }

    @Override
    public ProcessorNode findProcessor(final String id) {
        return processorMap.get(id);
    }

    @Override
    public List<ProcessorNode> findAllProcessors() {
        return new ArrayList<>(processorMap.values());
    }

    @Override
    public Label findLabel(final String id) {
        return null;
    }

    @Override
    public List<Label> findAllLabels() {
        return null;
    }

    @Override
    public Port findInputPort(final String id) {
        return null;
    }

    @Override
    public List<Port> findAllInputPorts() {
        return null;
    }

    @Override
    public Port getInputPortByName(final String name) {
        return null;
    }

    @Override
    public Port findOutputPort(final String id) {
        return null;
    }

    @Override
    public List<Port> findAllOutputPorts() {
        return null;
    }

    @Override
    public Port getOutputPortByName(final String name) {
        return null;
    }

    @Override
    public void addFunnel(final Funnel funnel) {

    }

    @Override
    public void addFunnel(final Funnel funnel, final boolean autoStart) {

    }

    @Override
    public Set<Funnel> getFunnels() {
        return null;
    }

    @Override
    public Funnel getFunnel(final String id) {
        return null;
    }

    @Override
    public List<Funnel> findAllFunnels() {
        return null;
    }

    @Override
    public void removeFunnel(final Funnel funnel) {

    }

    @Override
    public void addControllerService(final ControllerServiceNode service) {
        serviceMap.put(service.getIdentifier(), service);
        service.setProcessGroup(this);
    }

    @Override
    public ControllerServiceNode getControllerService(final String id) {
        return serviceMap.get(id);
    }

    @Override
    public Set<ControllerServiceNode> getControllerServices(final boolean recursive) {
        return new HashSet<>(serviceMap.values());
    }

    @Override
    public void removeControllerService(final ControllerServiceNode service) {
        serviceMap.remove(service.getIdentifier());
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void remove(final Snippet snippet) {

    }

    @Override
    public Connectable findConnectable(final String identifier) {
        return null;
    }

    @Override
    public void move(final Snippet snippet, final ProcessGroup destination) {

    }

    @Override
    public void verifyCanDelete() {

    }

    @Override
    public void verifyCanDelete(final boolean ignorePortConnections) {

    }

    @Override
    public void verifyCanStart() {

    }

    @Override
    public void verifyCanStop() {

    }

    @Override
    public void verifyCanDelete(final Snippet snippet) {

    }

    @Override
    public void verifyCanMove(final Snippet snippet, final ProcessGroup newProcessGroup) {

    }

    @Override
    public void verifyCanAddTemplate(String name) {
    }

    @Override
    public void addTemplate(final Template template) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeTemplate(final Template template) {
    }

    @Override
    public Template getTemplate(final String id) {
        return null;
    }

    @Override
    public Template findTemplate(final String id) {
        return null;
    }

    @Override
    public Set<Template> getTemplates() {
        return null;
    }

    @Override
    public Set<Template> findAllTemplates() {
        return null;
    }

    @Override
    public void verifyCanStart(final Connectable connectable) {
    }

    @Override
    public void verifyCanStop(final Connectable connectable) {
    }
}
