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
package org.apache.nifi.processors.standard.db.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;

/**
 * A DatabaseAdapter that generates Oracle-compliant SQL.
 */
public class OracleDatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Oracle";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Integer limit, Integer offset) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        final StringBuilder query = new StringBuilder();
        boolean nestedSelect = (limit != null || offset != null);
        if (nestedSelect) {
            // Need a nested SELECT query here in order to use ROWNUM to limit the results
            query.append("SELECT ");
            if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
                query.append("*");
            } else {
                query.append(columnNames);
            }
            query.append(" FROM (SELECT a.*, ROWNUM rnum FROM (");
        }

        query.append("SELECT ");
        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }
        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
        }
        if (!StringUtils.isEmpty(orderByClause)) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }
        if (nestedSelect) {
            query.append(") a");
            int offsetVal = 0;
            if (offset != null) {
                offsetVal = offset;
            }
            if (limit != null) {
                query.append(" WHERE ROWNUM <= ");
                query.append(offsetVal + limit);
            }
            query.append(") WHERE rnum > ");
            query.append(offsetVal);
        }
        return query.toString();
    }
}
