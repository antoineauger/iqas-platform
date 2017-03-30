<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<div id="new-remote-process-group-dialog" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">URL</div>
            <div class="setting-field">
                <input id="new-remote-process-group-uri" type="text" placeholder="https://remotehost:8080/nifi"/>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">
                Transport Protocol
                <div class="fa fa-question-circle" alt="Info" title="Specify the transport protocol to use for this Remote Process Group communication."></div>
            </div>
            <div class="setting-field">
                <div id="new-remote-process-group-transport-protocol-combo"></div>
            </div>
        </div>
        <div class="setting">
            <div class="remote-process-group-proxy-host-setting">
                <div class="setting-name">
                    HTTP Proxy server hostname
                    <div class="fa fa-question-circle" alt="Info" title="Specify the proxy server's hostname to use. If not specified, HTTP traffics are sent directly to the target NiFi instance."></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-host"/>
                </div>
            </div>
            <div class="remote-process-group-proxy-port-setting">
                <div class="setting-name">
                    HTTP Proxy server port
                    <div class="fa fa-question-circle" alt="Info" title="Specify the proxy server's port number, optional. If not specified, default port 80 will be used."></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-port"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-proxy-user-setting">
                <div class="setting-name">
                    HTTP Proxy user
                    <div class="fa fa-question-circle" alt="Info" title="Specify an user name to connect to the proxy server, optional."></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-user"/>
                </div>
            </div>
            <div class="remote-process-group-proxy-password-setting">
                <div class="setting-name">
                    HTTP Proxy password
                    <div class="fa fa-question-circle" alt="Info" title="Specify an user password to connect to the proxy server, optional."></div>
                </div>
                <div class="setting-field">
                    <input type="password" class="small-setting-input" id="new-remote-process-group-proxy-password"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-timeout-setting">
                <div class="setting-name">
                    Communications timeout
                    <div class="fa fa-question-circle" alt="Info" title="When communication with this remote process group takes longer than this amount of time, it will timeout."></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-timeout"/>
                </div>
            </div>
            <div class="remote-process-group-yield-duration-setting">
                <div class="setting-name">
                    Yield duration
                    <div class="fa fa-question-circle" alt="Info" title="When communication with this remote process group fails, it will not be scheduled again until this amount of time elapses."></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-yield-duration"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>