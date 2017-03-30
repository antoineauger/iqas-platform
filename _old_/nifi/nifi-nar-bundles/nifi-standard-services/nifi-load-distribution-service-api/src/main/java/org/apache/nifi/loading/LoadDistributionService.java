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
package org.apache.nifi.loading;

import java.util.Map;
import java.util.Set;

import org.apache.nifi.controller.ControllerService;

/**
 * A service that will provide a Map of Fully Qualified Domain Names (fqdn) with
 * their respective weights (scale of 1 - 100).
 */
public interface LoadDistributionService extends ControllerService {

    public Map<String, Integer> getLoadDistribution(Set<String> fqdns);

    public Map<String, Integer> getLoadDistribution(Set<String> fqdns, LoadDistributionListener listener);
}
