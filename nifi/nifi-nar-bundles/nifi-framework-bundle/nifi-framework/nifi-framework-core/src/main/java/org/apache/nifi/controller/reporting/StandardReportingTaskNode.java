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
package org.apache.nifi.controller.reporting;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;

public class StandardReportingTaskNode extends AbstractReportingTaskNode implements ReportingTaskNode {

    private final FlowController flowController;

    public StandardReportingTaskNode(final ReportingTask reportingTask, final String id, final FlowController controller,
                                     final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
                                     final VariableRegistry variableRegistry) {
        super(reportingTask, id, controller, processScheduler, validationContextFactory, variableRegistry);
        this.flowController = controller;
    }

    public StandardReportingTaskNode(final ReportingTask reportingTask, final String id, final FlowController controller,
        final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
        final String componentType, final String canonicalClassName, VariableRegistry variableRegistry) {
        super(reportingTask, id, controller, processScheduler, validationContextFactory, componentType, canonicalClassName,variableRegistry);
        this.flowController = controller;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return flowController;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ReportingTask, getIdentifier(), getName());
    }

    @Override
    public ReportingContext getReportingContext() {
        return new StandardReportingContext(flowController, flowController.getBulletinRepository(), getProperties(), flowController, getReportingTask(), variableRegistry);
    }
}
