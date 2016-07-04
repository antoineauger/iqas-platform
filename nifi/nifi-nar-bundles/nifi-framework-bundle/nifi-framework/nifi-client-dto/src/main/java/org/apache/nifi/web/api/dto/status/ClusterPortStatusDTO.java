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
import java.util.Collection;
import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * DTO for serializing the a port's status across the cluster.
 */
@XmlType(name = "clusterPortStatus")
public class ClusterPortStatusDTO {

    private Collection<NodePortStatusDTO> nodePortStatus;
    private Date statsLastRefreshed;
    private String portId;
    private String portName;

    /**
     * @return the time the status were last refreshed
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "The time the status was last refreshed."
    )
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    /**
     * @return port status from each node in the cluster
     */
    @ApiModelProperty(
            value = "The port status for each node."
    )
    public Collection<NodePortStatusDTO> getNodePortStatus() {
        return nodePortStatus;
    }

    public void setNodePortStatus(Collection<NodePortStatusDTO> nodePortStatus) {
        this.nodePortStatus = nodePortStatus;
    }

    /**
     * @return port id
     */
    @ApiModelProperty(
            value = "The id of the port."
    )
    public String getPortId() {
        return portId;
    }

    public void setPortId(String portId) {
        this.portId = portId;
    }

    /**
     * @return port name
     */
    @ApiModelProperty(
            value = "The name of the port."
    )
    public String getPortName() {
        return portName;
    }

    public void setPortName(String portName) {
        this.portName = portName;
    }

}
