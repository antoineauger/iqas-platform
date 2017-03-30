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
package org.apache.nifi.integration.util;

import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.resource.ResourceFactory;

/**
 *
 */
public class NiFiFlowTestAuthorizer implements Authorizer {

    public static final String NO_POLICY_COMPONENT_NAME = "No policies";

    public static final String PROXY_DN = "CN=localhost, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US";

    public static final String NONE_USER_DN = "none@nifi";
    public static final String READ_USER_DN = "read@nifi";
    public static final String WRITE_USER_DN = "write@nifi";
    public static final String READ_WRITE_USER_DN = "readwrite@nifi";

    public static final String TOKEN_USER = "user@nifi";

    /**
     * Creates a new FileAuthorizationProvider.
     */
    public NiFiFlowTestAuthorizer() {
    }

    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
    }

    @Override
    public AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
        // allow proxy
        if (ResourceFactory.getProxyResource().getIdentifier().equals(request.getResource().getIdentifier()) && PROXY_DN.equals(request.getIdentity())) {
            return AuthorizationResult.approved();
        }

        // read access
        if (READ_USER_DN.equals(request.getIdentity()) || READ_WRITE_USER_DN.equals(request.getIdentity())) {
            if (RequestAction.READ.equals(request.getAction())) {
                return AuthorizationResult.approved();
            }
        }

        // write access
        if (WRITE_USER_DN.equals(request.getIdentity()) || READ_WRITE_USER_DN.equals(request.getIdentity())) {
            if (RequestAction.WRITE.equals(request.getAction())) {
                return AuthorizationResult.approved();
            }
        }

        return AuthorizationResult.denied();
    }

    @Override
    public void preDestruction() {
    }

}
