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
package org.apache.nifi.web.api.dto.status;

import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.BulletinDTO;

/**
 * The status of this NiFi controller.
 */
@XmlType(name = "controllerStatus")
public class ControllerStatusDTO implements Cloneable {

    private Integer activeThreadCount = 0;
    private String queued;
    private Integer flowFilesQueued = 0;
    private Long bytesQueued = 0L;

    private String connectedNodes;
    private Integer connectedNodeCount = 0;
    private Integer totalNodeCount = 0;

    private Integer runningCount = 0;
    private Integer stoppedCount = 0;
    private Integer invalidCount = 0;
    private Integer disabledCount = 0;
    private Integer activeRemotePortCount = 0;
    private Integer inactiveRemotePortCount = 0;

    private List<BulletinDTO> bulletins;
    private List<BulletinDTO> controllerServiceBulletins;
    private List<BulletinDTO> reportingTaskBulletins;

    /**
     * The active thread count.
     *
     * @return The active thread count
     */
    @ApiModelProperty("The number of active threads in the NiFi.")
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return queue for the controller
     */
    @ApiModelProperty("The number of flowfilew queued in the NiFi.")
    public String getQueued() {
        return queued;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    /**
     * @return Used in clustering, will report the number of nodes connected vs
     * the number of nodes in the cluster
     */
    @ApiModelProperty("When clustered, reports the number of nodes connected vs the number of nodes in the cluster.")
    public String getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(String connectedNodes) {
        this.connectedNodes = connectedNodes;
    }

    /**
     * @return System bulletins to be reported to the user
     */
    @ApiModelProperty("System level bulletins to be reported to the user.")
    public List<BulletinDTO> getBulletins() {
        return bulletins;
    }

    public void setBulletins(List<BulletinDTO> bulletins) {
        this.bulletins = bulletins;
    }

    /**
     * @return Controller service bulletins to be reported to the user
     */
    @ApiModelProperty("Controller service bulletins to be reported to the user.")
    public List<BulletinDTO> getControllerServiceBulletins() {
        return controllerServiceBulletins;
    }

    public void setControllerServiceBulletins(List<BulletinDTO> controllerServiceBulletins) {
        this.controllerServiceBulletins = controllerServiceBulletins;
    }

    /**
     * @return Reporting task bulletins to be reported to the user
     */
    @ApiModelProperty("Reporting task bulletins to be reported to the user.")
    public List<BulletinDTO> getReportingTaskBulletins() {
        return reportingTaskBulletins;
    }

    public void setReportingTaskBulletins(List<BulletinDTO> reportingTaskBulletins) {
        this.reportingTaskBulletins = reportingTaskBulletins;
    }

    /**
     * @return number of running components in this controller
     */
    @ApiModelProperty("The number of running components in the NiFi.")
    public Integer getRunningCount() {
        return runningCount;
    }

    public void setRunningCount(Integer runningCount) {
        this.runningCount = runningCount;
    }

    /**
     * @return number of stopped components in this controller
     */
    @ApiModelProperty("The number of stopped components in the NiFi.")
    public Integer getStoppedCount() {
        return stoppedCount;
    }

    public void setStoppedCount(Integer stoppedCount) {
        this.stoppedCount = stoppedCount;
    }

    /**
     * @return number of invalid components in this controller
     */
    @ApiModelProperty("The number of invalid components in the NiFi.")
    public Integer getInvalidCount() {
        return invalidCount;
    }

    public void setInvalidCount(Integer invalidCount) {
        this.invalidCount = invalidCount;
    }

    /**
     * @return number of disabled components in this controller
     */
    @ApiModelProperty("The number of disabled components in the NiFi.")
    public Integer getDisabledCount() {
        return disabledCount;
    }

    public void setDisabledCount(Integer disabledCount) {
        this.disabledCount = disabledCount;
    }

    /**
     * @return number of active remote ports in this controller
     */
    @ApiModelProperty("The number of active remote ports in the NiFi.")
    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    /**
     * @return number of inactive remote ports in this controller
     */
    @ApiModelProperty("The number of inactive remote ports in the NiFi.")
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    @ApiModelProperty("The number of FlowFiles queued across the entire flow")
    public Integer getFlowFilesQueued() {
        return flowFilesQueued;
    }

    public void setFlowFilesQueued(Integer flowFilesQueued) {
        this.flowFilesQueued = flowFilesQueued;
    }

    @ApiModelProperty("The size of the FlowFiles queued across the entire flow")
    public Long getBytesQueued() {
        return bytesQueued;
    }

    public void setBytesQueued(Long bytesQueued) {
        this.bytesQueued = bytesQueued;
    }

    @ApiModelProperty("The number of nodes that are currently connected to the cluster")
    public Integer getConnectedNodeCount() {
        return connectedNodeCount;
    }

    public void setConnectedNodeCount(Integer connectedNodeCount) {
        this.connectedNodeCount = connectedNodeCount;
    }

    @ApiModelProperty("The number of nodes in the cluster, regardless of whether or not they are connected")
    public Integer getTotalNodeCount() {
        return totalNodeCount;
    }

    public void setTotalNodeCount(Integer totalNodeCount) {
        this.totalNodeCount = totalNodeCount;
    }

    @Override
    public ControllerStatusDTO clone() {
        final ControllerStatusDTO other = new ControllerStatusDTO();
        other.setActiveThreadCount(getActiveThreadCount());
        other.setQueued(getQueued());
        other.setFlowFilesQueued(getFlowFilesQueued());
        other.setBytesQueued(getBytesQueued());
        other.setConnectedNodes(getConnectedNodes());
        other.setConnectedNodeCount(getConnectedNodeCount());
        other.setTotalNodeCount(getTotalNodeCount());
        other.setRunningCount(getRunningCount());
        other.setStoppedCount(getStoppedCount());
        other.setInvalidCount(getInvalidCount());
        other.setDisabledCount(getDisabledCount());
        other.setActiveRemotePortCount(getActiveRemotePortCount());
        other.setInactiveRemotePortCount(getInactiveRemotePortCount());
        other.setBulletins(getBulletins() == null ? null : new ArrayList<>(getBulletins()));
        other.setControllerServiceBulletins(getControllerServiceBulletins() == null ? null : new ArrayList<>(getControllerServiceBulletins()));
        other.setReportingTaskBulletins(getReportingTaskBulletins() == null ? null : new ArrayList<>(getReportingTaskBulletins()));
        return other;
    }
}
