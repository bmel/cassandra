/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

/**
 * Encapsulates a set of {@link Permission}, along with any permission constraints.
 */
public class PermissionSet
{
    private final Set<Permission> permissions;
    private final Map<Permission, Set<String>> permissionColumns;

    PermissionSet(Set<Permission> permissions, Map<Permission, Set<String>> permissionColumns)
    {
        this.permissions = permissions;
        this.permissionColumns = permissionColumns;
    }

    PermissionSet(Set<Permission> permissions)
    {
        this(permissions, Collections.emptyMap());
    }

    public Set<Permission> getPermissions()
    {
        return permissions;
    }

    public Map<Permission, Set<String>> getPermissionColumns()
    {
        return permissionColumns;
    }

    public String toString()
    {
        return "PermissionSet{" +
               "permissions=" + permissions +
               ", permissionColumns=" + permissionColumns +
               '}';
    }

    public boolean hasColumnConstraint(Permission perm)
    {
        Set<String> cols = permissionColumns.get(perm);
        return cols != null && !cols.isEmpty();
    }
}
