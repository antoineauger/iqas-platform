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
package org.apache.nifi.authorization;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestAccessPolicy {

    static final String TEST_RESOURCE = "1";

    @Test
    public void testSimpleCreation() {
        final String identifier = "1";
        final String user1 = "user1";
        final String user2 = "user2";
        final RequestAction action = RequestAction.READ;

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier(identifier)
                .resource(TEST_RESOURCE)
                .addUser(user1)
                .addUser(user2)
                .action(action)
                .build();

        assertEquals(identifier, policy.getIdentifier());

        assertNotNull(policy.getResource());
        assertEquals(TEST_RESOURCE, policy.getResource());

        assertNotNull(policy.getUsers());
        assertEquals(2, policy.getUsers().size());
        assertTrue(policy.getUsers().contains(user1));
        assertTrue(policy.getUsers().contains(user2));

        assertNotNull(policy.getAction());
        assertEquals(RequestAction.READ, policy.getAction());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingIdentifier() {
        new AccessPolicy.Builder()
                .resource(TEST_RESOURCE)
                .addUser("user1")
                .action(RequestAction.READ)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingResource() {
        new AccessPolicy.Builder()
                .identifier("1")
                .addUser("user1")
                .action(RequestAction.READ)
                .build();
    }

    @Test
    public void testMissingUsersAndGroups() {
        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .build();

        assertNotNull(policy);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingActions() {
        new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addUser("user1")
                .build();
    }

    @Test
    public void testFromPolicy() {
        final String identifier = "1";
        final String user1 = "user1";
        final String user2 = "user2";
        final String group1 = "group1";
        final String group2 = "group2";
        final RequestAction action = RequestAction.READ;

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier(identifier)
                .resource(TEST_RESOURCE)
                .addUser(user1)
                .addUser(user2)
                .addGroup(group1)
                .addGroup(group2)
                .action(action)
                .build();

        assertEquals(identifier, policy.getIdentifier());

        assertNotNull(policy.getResource());
        assertEquals(TEST_RESOURCE, policy.getResource());

        assertNotNull(policy.getUsers());
        assertEquals(2, policy.getUsers().size());
        assertTrue(policy.getUsers().contains(user1));
        assertTrue(policy.getUsers().contains(user2));

        assertNotNull(policy.getGroups());
        assertEquals(2, policy.getGroups().size());
        assertTrue(policy.getGroups().contains(group1));
        assertTrue(policy.getGroups().contains(group2));

        assertNotNull(policy.getAction());
        assertEquals(RequestAction.READ, policy.getAction());

        final AccessPolicy policy2 = new AccessPolicy.Builder(policy).build();
        assertEquals(policy.getIdentifier(), policy2.getIdentifier());
        assertEquals(policy.getResource(), policy2.getResource());
        assertEquals(policy.getUsers(), policy2.getUsers());
        assertEquals(policy.getAction(), policy2.getAction());
    }

    @Test(expected = IllegalStateException.class)
    public void testFromPolicyAndChangeIdentifier() {
        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addUser("user1")
                .action(RequestAction.READ)
                .build();

        new AccessPolicy.Builder(policy).identifier("2").build();
    }

    @Test
    public void testAddRemoveClearUsers() {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addUser("user1")
                .action(RequestAction.READ);

        final AccessPolicy policy1 = builder.build();
        assertEquals(1, policy1.getUsers().size());
        assertTrue(policy1.getUsers().contains("user1"));

        final Set<String> moreEntities = new HashSet<>();
        moreEntities.add("user2");
        moreEntities.add("user3");
        moreEntities.add("user4");

        final AccessPolicy policy2 = builder.addUsers(moreEntities).build();
        assertEquals(4, policy2.getUsers().size());
        assertTrue(policy2.getUsers().contains("user1"));
        assertTrue(policy2.getUsers().contains("user2"));
        assertTrue(policy2.getUsers().contains("user3"));
        assertTrue(policy2.getUsers().contains("user4"));

        final AccessPolicy policy3 = builder.removeUser("user3").build();
        assertEquals(3, policy3.getUsers().size());
        assertTrue(policy3.getUsers().contains("user1"));
        assertTrue(policy3.getUsers().contains("user2"));
        assertTrue(policy3.getUsers().contains("user4"));

        final Set<String> removeEntities = new HashSet<>();
        removeEntities.add("user1");
        removeEntities.add("user4");

        final AccessPolicy policy4 = builder.removeUsers(removeEntities).build();
        assertEquals(1, policy4.getUsers().size());
        assertTrue(policy4.getUsers().contains("user2"));


        final AccessPolicy policy5 = builder.clearUsers().build();
        assertEquals(0, policy5.getUsers().size());
    }

    @Test
    public void testAddRemoveClearGroups() {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addGroup("group1")
                .action(RequestAction.READ);

        final AccessPolicy policy1 = builder.build();
        assertEquals(1, policy1.getGroups().size());
        assertTrue(policy1.getGroups().contains("group1"));

        final Set<String> moreGroups = new HashSet<>();
        moreGroups.add("group2");
        moreGroups.add("group3");
        moreGroups.add("group4");

        final AccessPolicy policy2 = builder.addGroups(moreGroups).build();
        assertEquals(4, policy2.getGroups().size());
        assertTrue(policy2.getGroups().contains("group1"));
        assertTrue(policy2.getGroups().contains("group2"));
        assertTrue(policy2.getGroups().contains("group3"));
        assertTrue(policy2.getGroups().contains("group4"));

        final AccessPolicy policy3 = builder.removeGroup("group3").build();
        assertEquals(3, policy3.getGroups().size());
        assertTrue(policy3.getGroups().contains("group1"));
        assertTrue(policy3.getGroups().contains("group2"));
        assertTrue(policy3.getGroups().contains("group4"));

        final Set<String> removeGroups = new HashSet<>();
        removeGroups.add("group1");
        removeGroups.add("group4");

        final AccessPolicy policy4 = builder.removeGroups(removeGroups).build();
        assertEquals(1, policy4.getGroups().size());
        assertTrue(policy4.getGroups().contains("group2"));

        final AccessPolicy policy5 = builder.clearGroups().build();
        assertEquals(0, policy5.getUsers().size());
    }

}
