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
package org.apache.cassandra.cql3.statements;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionSpec;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantPermissionsStatement extends PermissionsManagementStatement
{
    private final Map<Permission, Set<ColumnIdentifier>> permissionColumns;

    public GrantPermissionsStatement(Set<Permission> permissions,
                                     Map<Permission, Set<ColumnIdentifier>> permissionColumns,
                                     IResource resource,
                                     RoleName grantee)
    {
        super(permissions, resource, grantee);
        this.permissionColumns = permissionColumns == null ? Collections.emptyMap() : permissionColumns;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        super.validate(state);
        validateColumnsExist();
    }

    private void validateColumnsExist()
    {
        if (!permissionColumns.isEmpty())
        {
            if (resource instanceof DataResource)
            {
                DataResource dataResource = (DataResource) resource;
                // Throws exception when not a table:
                Collection<ColumnDefinition> columnDefinitions = dataResource.getTableColumns();
                Set<ColumnIdentifier> existingColumns = columnDefinitions.stream()
                                                                         .map(c -> c.name).collect(Collectors.toSet());
                for (Permission permission : permissions)
                {
                    Set<ColumnIdentifier> missingColumns = permissionColumns.get(permission).stream()
                                                                            .filter(c -> !existingColumns.contains(c))
                                                                            .collect(Collectors.toSet());
                    if (!missingColumns.isEmpty())
                    {
                        throw new InvalidRequestException(String.format("Column(s) %s do not exist in %s",
                                                                        missingColumns,
                                                                        resource));
                    }
                }
            }
            else
            {
                throw new InvalidRequestException("Column lists are only applicable to permissions on tables");
            }
        }
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        DatabaseDescriptor.getAuthorizer().grant(state.getUser(),
                                                 new PermissionSpec(permissions, permissionColumns, resource, grantee));
        return null;
    }

}
