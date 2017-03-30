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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

public class StandardProcessorDAO extends ComponentDAO implements ProcessorDAO {

    private static final Logger logger = LoggerFactory.getLogger(StandardProcessorDAO.class);
    private FlowController flowController;
    private ComponentStateDAO componentStateDAO;

    private ProcessorNode locateProcessor(final String processorId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        final ProcessorNode processor = rootGroup.findProcessor(processorId);

        if (processor == null) {
            throw new ResourceNotFoundException(String.format("Unable to find processor with id '%s'.", processorId));
        } else {
            return processor;
        }
    }

    @Override
    public boolean hasProcessor(String id) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        return rootGroup.findProcessor(id) != null;
    }

    @Override
    public ProcessorNode createProcessor(final String groupId, ProcessorDTO processorDTO) {
        if (processorDTO.getParentGroupId() != null && !flowController.areGroupsSame(groupId, processorDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Processor is being added.");
        }

        // ensure the type is specified
        if (processorDTO.getType() == null) {
            throw new IllegalArgumentException("The processor type must be specified.");
        }

        // get the group to add the processor to
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        try {
            // attempt to create the processor
            ProcessorNode processor = flowController.createProcessor(processorDTO.getType(), processorDTO.getId());

            // ensure we can perform the update before we add the processor to the flow
            verifyUpdate(processor, processorDTO);

            // add the processor to the group
            group.addProcessor(processor);

            // configure the processor
            configureProcessor(processor, processorDTO);

            return processor;
        } catch (ProcessorInstantiationException pse) {
            throw new NiFiCoreException(String.format("Unable to create processor of type %s due to: %s", processorDTO.getType(), pse.getMessage()), pse);
        } catch (IllegalStateException | ComponentLifeCycleException ise) {
            throw new NiFiCoreException(ise.getMessage(), ise);
        }
    }

    private void configureProcessor(final ProcessorNode processor, final ProcessorDTO processorDTO) {
        final ProcessorConfigDTO config = processorDTO.getConfig();

        // ensure some configuration was specified
        if (isNotNull(config)) {
            // perform the configuration
            final String schedulingStrategy = config.getSchedulingStrategy();
            final String executionNode = config.getExecutionNode();
            final String comments = config.getComments();
            final String annotationData = config.getAnnotationData();
            final Integer maxTasks = config.getConcurrentlySchedulableTaskCount();
            final Map<String, String> configProperties = config.getProperties();
            final String schedulingPeriod = config.getSchedulingPeriod();
            final String penaltyDuration = config.getPenaltyDuration();
            final String yieldDuration = config.getYieldDuration();
            final Long runDurationMillis = config.getRunDurationMillis();
            final String bulletinLevel = config.getBulletinLevel();
            final Set<String> undefinedRelationshipsToTerminate = config.getAutoTerminatedRelationships();

            // ensure scheduling strategy is set first
            if (isNotNull(schedulingStrategy)) {
                processor.setSchedulingStrategy(SchedulingStrategy.valueOf(schedulingStrategy));
            }

            if (isNotNull(executionNode)) {
                processor.setExecutionNode(ExecutionNode.valueOf(executionNode));
            }
            if (isNotNull(comments)) {
                processor.setComments(comments);
            }
            if (isNotNull(annotationData)) {
                processor.setAnnotationData(annotationData);
            }
            if (isNotNull(maxTasks)) {
                processor.setMaxConcurrentTasks(maxTasks);
            }
            if (isNotNull(schedulingPeriod)) {
                processor.setScheduldingPeriod(schedulingPeriod);
            }
            if (isNotNull(penaltyDuration)) {
                processor.setPenalizationPeriod(penaltyDuration);
            }
            if (isNotNull(yieldDuration)) {
                processor.setYieldPeriod(yieldDuration);
            }
            if (isNotNull(runDurationMillis)) {
                processor.setRunDuration(runDurationMillis, TimeUnit.MILLISECONDS);
            }
            if (isNotNull(bulletinLevel)) {
                processor.setBulletinLevel(LogLevel.valueOf(bulletinLevel));
            }
            if (isNotNull(config.isLossTolerant())) {
                processor.setLossTolerant(config.isLossTolerant());
            }
            if (isNotNull(configProperties)) {
                processor.setProperties(configProperties);
            }

            if (isNotNull(undefinedRelationshipsToTerminate)) {
                final Set<Relationship> relationships = new HashSet<>();
                for (final String relName : undefinedRelationshipsToTerminate) {
                    relationships.add(new Relationship.Builder().name(relName).build());
                }
                processor.setAutoTerminatedRelationships(relationships);
            }
        }

        // update processor settings
        if (isNotNull(processorDTO.getPosition())) {
            processor.setPosition(new Position(processorDTO.getPosition().getX(), processorDTO.getPosition().getY()));
        }

        if (isNotNull(processorDTO.getStyle())) {
            processor.setStyle(processorDTO.getStyle());
        }

        final String name = processorDTO.getName();
        if (isNotNull(name)) {
            processor.setName(name);
        }
    }

    private List<String> validateProposedConfiguration(final ProcessorNode processorNode, final ProcessorConfigDTO config) {
        List<String> validationErrors = new ArrayList<>();

        // validate settings
        if (isNotNull(config.getPenaltyDuration())) {
            Matcher penaltyMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(config.getPenaltyDuration());
            if (!penaltyMatcher.matches()) {
                validationErrors.add("Penalty duration is not a valid time duration (ie 30 sec, 5 min)");
            }
        }
        if (isNotNull(config.getYieldDuration())) {
            Matcher yieldMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(config.getYieldDuration());
            if (!yieldMatcher.matches()) {
                validationErrors.add("Yield duration is not a valid time duration (ie 30 sec, 5 min)");
            }
        }
        if (isNotNull(config.getBulletinLevel())) {
            try {
                LogLevel.valueOf(config.getBulletinLevel());
            } catch (IllegalArgumentException iae) {
                validationErrors.add(String.format("Bulletin level: Value must be one of [%s]", StringUtils.join(LogLevel.values(), ", ")));
            }
        }
        if (isNotNull(config.getExecutionNode())) {
            try {
                ExecutionNode.valueOf(config.getExecutionNode());
            } catch (IllegalArgumentException iae) {
                validationErrors.add(String.format("Execution node: Value must be one of [%s]", StringUtils.join(ExecutionNode.values(), ", ")));
            }
        }

        // get the current scheduling strategy
        SchedulingStrategy schedulingStrategy = processorNode.getSchedulingStrategy();

        // validate the new scheduling strategy if appropriate
        if (isNotNull(config.getSchedulingStrategy())) {
            try {
                // this will be the new scheduling strategy so use it
                schedulingStrategy = SchedulingStrategy.valueOf(config.getSchedulingStrategy());
            } catch (IllegalArgumentException iae) {
                validationErrors.add(String.format("Scheduling strategy: Value must be one of [%s]", StringUtils.join(SchedulingStrategy.values(), ", ")));
            }
        }

        // validate the concurrent tasks based on the scheduling strategy
        if (isNotNull(config.getConcurrentlySchedulableTaskCount())) {
            switch (schedulingStrategy) {
                case TIMER_DRIVEN:
                case PRIMARY_NODE_ONLY:
                    if (config.getConcurrentlySchedulableTaskCount() <= 0) {
                        validationErrors.add("Concurrent tasks must be greater than 0.");
                    }
                    break;
                case EVENT_DRIVEN:
                    if (config.getConcurrentlySchedulableTaskCount() < 0) {
                        validationErrors.add("Concurrent tasks must be greater or equal to 0.");
                    }
                    break;
            }
        }

        // validate the scheduling period based on the scheduling strategy
        if (isNotNull(config.getSchedulingPeriod())) {
            switch (schedulingStrategy) {
                case TIMER_DRIVEN:
                case PRIMARY_NODE_ONLY:
                    final Matcher schedulingMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(config.getSchedulingPeriod());
                    if (!schedulingMatcher.matches()) {
                        validationErrors.add("Scheduling period is not a valid time duration (ie 30 sec, 5 min)");
                    }
                    break;
                case CRON_DRIVEN:
                    try {
                        new CronExpression(config.getSchedulingPeriod());
                    } catch (final ParseException pe) {
                        throw new IllegalArgumentException(String.format("Scheduling Period '%s' is not a valid cron expression: %s", config.getSchedulingPeriod(), pe.getMessage()));
                    } catch (final Exception e) {
                        throw new IllegalArgumentException("Scheduling Period is not a valid cron expression: " + config.getSchedulingPeriod());
                    }
                    break;
            }
        }

        final Set<String> autoTerminatedRelationships = config.getAutoTerminatedRelationships();
        if (isNotNull(autoTerminatedRelationships)) {
            for (final String relationshipName : autoTerminatedRelationships) {
                final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
                final Set<Connection> connections = processorNode.getConnections(relationship);
                if (isNotNull(connections) && !connections.isEmpty()) {
                    validationErrors.add("Cannot automatically terminate '" + relationshipName + "' relationship because a Connection already exists with this relationship");
                }
            }
        }

        return validationErrors;
    }

    @Override
    public ProcessorNode getProcessor(final String id) {
        return locateProcessor(id);
    }

    @Override
    public Set<ProcessorNode> getProcessors(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getProcessors();
    }

    @Override
    public void verifyUpdate(final ProcessorDTO processorDTO) {
        verifyUpdate(locateProcessor(processorDTO.getId()), processorDTO);
    }

    private void verifyUpdate(ProcessorNode processor, ProcessorDTO processorDTO) {
        // ensure the state, if specified, is valid
        if (isNotNull(processorDTO.getState())) {
            try {
                final ScheduledState purposedScheduledState = ScheduledState.valueOf(processorDTO.getState());

                // only attempt an action if it is changing
                if (!purposedScheduledState.equals(processor.getScheduledState())) {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            processor.verifyCanStart();
                            break;
                        case STOPPED:
                            switch (processor.getScheduledState()) {
                                case RUNNING:
                                    processor.verifyCanStop();
                                    break;
                                case DISABLED:
                                    processor.verifyCanEnable();
                                    break;
                            }
                            break;
                        case DISABLED:
                            processor.verifyCanDisable();
                            break;
                    }
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(String.format(
                        "The specified processor state (%s) is not valid. Valid options are 'RUNNING', 'STOPPED', and 'DISABLED'.",
                        processorDTO.getState()));
            }
        }

        boolean modificationRequest = false;
        if (isAnyNotNull(processorDTO.getName())) {
            modificationRequest = true;
        }

        final ProcessorConfigDTO configDTO = processorDTO.getConfig();
        if (configDTO != null) {
            if (isAnyNotNull(configDTO.getAnnotationData(),
                    configDTO.getAutoTerminatedRelationships(),
                    configDTO.getBulletinLevel(),
                    configDTO.getComments(),
                    configDTO.getConcurrentlySchedulableTaskCount(),
                    configDTO.getPenaltyDuration(),
                    configDTO.getProperties(),
                    configDTO.getSchedulingPeriod(),
                    configDTO.getSchedulingStrategy(),
                    configDTO.getExecutionNode(),
                    configDTO.getYieldDuration())) {

                modificationRequest = true;
            }

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(processor, configDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }
        }

        if (modificationRequest) {
            processor.verifyCanUpdate();
        }
    }

    @Override
    public ProcessorNode updateProcessor(ProcessorDTO processorDTO) {
        ProcessorNode processor = locateProcessor(processorDTO.getId());
        ProcessGroup parentGroup = processor.getProcessGroup();

        // ensure we can perform the update
        verifyUpdate(processor, processorDTO);

        // configure the processor
        configureProcessor(processor, processorDTO);

        // see if an update is necessary
        if (isNotNull(processorDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(processorDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(processor.getScheduledState())) {
                try {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            parentGroup.startProcessor(processor);
                            break;
                        case STOPPED:
                            switch (processor.getScheduledState()) {
                                case RUNNING:
                                    parentGroup.stopProcessor(processor);
                                    break;
                                case DISABLED:
                                    parentGroup.enableProcessor(processor);
                                    break;
                            }
                            break;
                        case DISABLED:
                            parentGroup.disableProcessor(processor);
                            break;
                    }
                } catch (IllegalStateException | ComponentLifeCycleException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                } catch (RejectedExecutionException ree) {
                    throw new NiFiCoreException("Unable to schedule all tasks for the specified processor.", ree);
                } catch (NullPointerException npe) {
                    throw new NiFiCoreException("Unable to update processor run state.", npe);
                } catch (Exception e) {
                    throw new NiFiCoreException("Unable to update processor run state: " + e, e);
                }
            }
        }

        return processor;
    }

    @Override
    public void verifyDelete(String processorId) {
        ProcessorNode processor = locateProcessor(processorId);
        processor.verifyCanDelete();
    }

    @Override
    public void deleteProcessor(String processorId) {
        // get the group and the processor
        ProcessorNode processor = locateProcessor(processorId);

        try {
            // attempt remove the processor
            processor.getProcessGroup().removeProcessor(processor);
        } catch (ComponentLifeCycleException plce) {
            throw new NiFiCoreException(plce.getMessage(), plce);
        }
    }

    @Override
    public StateMap getState(String processorId, final Scope scope) {
        final ProcessorNode processor = locateProcessor(processorId);
        return componentStateDAO.getState(processor, scope);
    }

    @Override
    public void verifyClearState(String processorId) {
        final ProcessorNode processor = locateProcessor(processorId);
        processor.verifyCanClearState();
    }

    @Override
    public void clearState(String processorId) {
        final ProcessorNode processor = locateProcessor(processorId);
        componentStateDAO.clearState(processor);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

    public void setComponentStateDAO(ComponentStateDAO componentStateDAO) {
        this.componentStateDAO = componentStateDAO;
    }
}
