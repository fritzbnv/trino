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
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Supplies ClickHouse-native write sinks in place of the default JDBC ones. INSERT/CTAS and MERGE both stream rows to
 * ClickHouse in RowBinary via {@link ClickHouseNativePageSink} (Client V2), which is far faster than the base
 * {@link io.trino.plugin.jdbc.JdbcPageSink}'s per-batch {@code PreparedStatement.executeBatch()}. MERGE remains an
 * INSERT-only upsert against a ReplacingMergeTree target via {@link ClickHouseMergeSink}. Bound in place of the default
 * provider by {@link ClickHouseClientModule}.
 */
public class ClickHousePageSinkProvider
        extends JdbcPageSinkProvider
{
    private final JdbcClient jdbcClient;
    private final String connectionUrl;
    private final CredentialProvider credentialProvider;

    @Inject
    public ClickHousePageSinkProvider(
            JdbcClient jdbcClient,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider)
    {
        super(jdbcClient, remoteQueryModifier, queryBuilder);
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.connectionUrl = requireNonNull(config, "config is null").getConnectionUrl();
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createNativePageSink(session, (JdbcOutputTableHandle) tableHandle, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createNativePageSink(session, (JdbcOutputTableHandle) tableHandle, pageSinkId);
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        ClickHouseNativePageSink insertSink = createNativePageSink(session, ((JdbcMergeTableHandle) mergeHandle).getOutputTableHandle(), pageSinkId);
        return new ClickHouseMergeSink(session, (JdbcMergeTableHandle) mergeHandle, pageSinkId, insertSink);
    }

    private ClickHouseNativePageSink createNativePageSink(ConnectorSession session, JdbcOutputTableHandle handle, ConnectorPageSinkId pageSinkId)
    {
        Optional<ConnectorIdentity> identity = Optional.of(session.getIdentity());
        return new ClickHouseNativePageSink(
                session,
                handle,
                jdbcClient,
                pageSinkId,
                connectionUrl,
                credentialProvider.getConnectionUser(identity),
                credentialProvider.getConnectionPassword(identity));
    }
}
