/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

/**
 * Reuses the default JDBC insert page sinks but supplies a ClickHouse-specific {@link ClickHouseMergeSink} (INSERT-only
 * against a ReplacingMergeTree target) for MERGE. Bound in place of the default provider by {@link ClickHouseClientModule}.
 */
public class ClickHousePageSinkProvider
        extends JdbcPageSinkProvider
{
    private final JdbcClient jdbcClient;
    private final RemoteQueryModifier queryModifier;

    @Inject
    public ClickHousePageSinkProvider(JdbcClient jdbcClient, RemoteQueryModifier remoteQueryModifier, QueryBuilder queryBuilder)
    {
        super(jdbcClient, remoteQueryModifier, queryBuilder);
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.queryModifier = requireNonNull(remoteQueryModifier, "remoteQueryModifier is null");
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            ConnectorPageSinkId pageSinkId)
    {
        return new ClickHouseMergeSink(session, (JdbcMergeTableHandle) mergeHandle, jdbcClient, pageSinkId, queryModifier);
    }
}
