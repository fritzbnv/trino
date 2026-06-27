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

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.RowType;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.DefaultJdbcMetadata.MERGE_ROW_ID;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static java.util.Objects.requireNonNull;

/**
 * ClickHouse-specific metadata. It changes two things from {@link DefaultJdbcMetadata}, both for MERGE: ClickHouse
 * implements MERGE as INSERT-only against a ReplacingMergeTree target (see {@link ClickHouseMergeSink}), so it uses
 * {@link RowChangeParadigm#DELETE_ROW_AND_INSERT_ROW}. Under that paradigm the engine turns every matched-update into a
 * delete-row plus a full insert-row; the merge sink keeps only the insert rows and relies on ReplacingMergeTree to
 * collapse duplicates by ORDER BY key on background merge.
 * <p>
 * Because ClickHouse exposes no JDBC primary keys, the merge row id cannot be derived from a primary key (which would
 * leave the base implementation returning a scalar {@code bigint} row id that is incompatible with the
 * {@code DELETE_ROW_AND_INSERT_ROW} merge processor). The row id is therefore a {@link RowType} built from all table
 * columns. Its contents are never used by {@link ClickHouseMergeSink} (which only inserts), but it must be a row type
 * so the merge row blocks line up.
 */
public class ClickHouseMetadata
        extends DefaultJdbcMetadata
{
    private final JdbcClient jdbcClient;

    public ClickHouseMetadata(
            JdbcClient jdbcClient,
            TimestampTimeZoneDomain timestampTimeZoneDomain,
            boolean precalculateStatisticsForPushdown,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, timestampTimeZoneDomain, precalculateStatisticsForPushdown, jdbcQueryEventListeners);
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        List<RowType.Field> rowIdFields = jdbcClient.getColumns(
                        session,
                        handle.getRequiredNamedRelation().getSchemaTableName(),
                        handle.getRequiredNamedRelation().getRemoteTableName()).stream()
                .map(column -> new RowType.Field(Optional.of(column.getColumnName()), column.getColumnType()))
                .collect(toImmutableList());

        return new JdbcColumnHandle(
                MERGE_ROW_ID,
                new JdbcTypeHandle(Types.ROWID, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                RowType.from(rowIdFields));
    }
}
