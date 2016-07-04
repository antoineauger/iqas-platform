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
package org.apache.nifi.remote.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSiteToSiteClient implements SiteToSiteClient {

    protected final SiteToSiteClientConfig config;
    protected final SiteInfoProvider siteInfoProvider;
    protected final URI clusterUrl;

    public AbstractSiteToSiteClient(final SiteToSiteClientConfig config) {
        this.config = config;

        try {
            Objects.requireNonNull(config.getUrl(), "URL cannot be null");
            clusterUrl = new URI(config.getUrl());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Invalid Cluster URL: " + config.getUrl());
        }

        final int commsTimeout = (int) config.getTimeout(TimeUnit.MILLISECONDS);
        siteInfoProvider = new SiteInfoProvider();
        siteInfoProvider.setClusterUrl(clusterUrl);
        siteInfoProvider.setSslContext(config.getSslContext());
        siteInfoProvider.setConnectTimeoutMillis(commsTimeout);
        siteInfoProvider.setReadTimeoutMillis(commsTimeout);
        siteInfoProvider.setProxy(config.getHttpProxy());

    }

    @Override
    public SiteToSiteClientConfig getConfig() {
        return config;
    }
}