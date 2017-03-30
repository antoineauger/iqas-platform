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
package org.apache.nifi.remote.cluster;

public class NodeInformation {

    private final String siteToSiteHostname;
    private final Integer siteToSitePort;
    private final Integer siteToSiteHttpApiPort;
    private final int apiPort;
    private final boolean isSiteToSiteSecure;
    private final int totalFlowFiles;

    public NodeInformation(final String siteToSiteHostname, final Integer siteToSitePort, final Integer siteToSiteHttpApiPort,
            final int apiPort, final boolean isSiteToSiteSecure, final int totalFlowFiles) {
        this.siteToSiteHostname = siteToSiteHostname;
        this.siteToSitePort = siteToSitePort;
        this.siteToSiteHttpApiPort = siteToSiteHttpApiPort;
        this.apiPort = apiPort;
        this.isSiteToSiteSecure = isSiteToSiteSecure;
        this.totalFlowFiles = totalFlowFiles;
    }

    public String getSiteToSiteHostname() {
        return siteToSiteHostname;
    }

    public int getAPIPort() {
        return apiPort;
    }

    public Integer getSiteToSitePort() {
        return siteToSitePort;
    }

    public Integer getSiteToSiteHttpApiPort() {
        return siteToSiteHttpApiPort;
    }

    public boolean isSiteToSiteSecure() {
        return isSiteToSiteSecure;
    }

    public int getTotalFlowFiles() {
        return totalFlowFiles;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NodeInformation)) {
            return false;
        }

        final NodeInformation other = (NodeInformation) obj;
        if (!siteToSiteHostname.equals(other.siteToSiteHostname)) {
            return false;
        }
        if (siteToSitePort == null && other.siteToSitePort != null) {
            return false;
        }
        if (siteToSitePort != null && other.siteToSitePort == null) {
            return false;
        } else if (siteToSitePort != null && siteToSitePort.intValue() != other.siteToSitePort.intValue()) {
            return false;
        }

        if (siteToSiteHttpApiPort == null && other.siteToSiteHttpApiPort != null) {
            return false;
        }
        if (siteToSiteHttpApiPort != null && other.siteToSiteHttpApiPort == null) {
            return false;
        } else if (siteToSiteHttpApiPort != null && siteToSiteHttpApiPort.intValue() != other.siteToSiteHttpApiPort.intValue()) {
            return false;
        }

        if (apiPort != other.apiPort) {
            return false;
        }
        if (isSiteToSiteSecure != other.isSiteToSiteSecure) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 83832 + siteToSiteHostname.hashCode() + (siteToSitePort == null ? 8 : siteToSitePort.hashCode()) + apiPort + (isSiteToSiteSecure ? 3829 : 0);
    }

    @Override
    public String toString() {
        return "Node[" + siteToSiteHostname + ":" + apiPort + "]";
    }
}
