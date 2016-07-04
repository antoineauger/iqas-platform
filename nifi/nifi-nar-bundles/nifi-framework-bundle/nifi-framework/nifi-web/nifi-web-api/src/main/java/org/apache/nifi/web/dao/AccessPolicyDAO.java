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
package org.apache.nifi.web.dao;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;

public interface AccessPolicyDAO {

    /**
     * @param accessPolicyId access policy ID
     * @return Determines if the specified access policy exists
     */
    boolean hasAccessPolicy(String accessPolicyId);

    /**
     * Creates an access policy.
     *
     * @param accessPolicyDTO The access policy DTO
     * @return The access policy transfer object
     */
    AccessPolicy createAccessPolicy(AccessPolicyDTO accessPolicyDTO);

    /**
     * Gets the acess policy with the specified ID.
     *
     * @param accessPolicyId The access policy ID
     * @return The access policy transfer object
     */
    AccessPolicy getAccessPolicy(String accessPolicyId);

    /**
     * Updates the specified access policy.
     *
     * @param accessPolicyDTO The access policy DTO
     * @return The access policy transfer object
     */
    AccessPolicy updateAccessPolicy(AccessPolicyDTO accessPolicyDTO);

    /**
     * Deletes the specified access policy.
     *
     * @param accessPolicyId The access policy ID
     * @return The access policy transfer object of the deleted access policy
     */
    AccessPolicy deleteAccessPolicy(String accessPolicyId);


}
