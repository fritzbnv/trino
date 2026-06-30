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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import dev.failsafe.RetryPolicy;
import io.trino.plugin.jdbc.BaseJdbcConnectorTableHandle;
import io.trino.plugin.jdbc.ForJdbcClient;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcPageSource;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.MergeJdbcPageSource;
import io.trino.plugin.jdbc.MergeJdbcPageSource.ColumnAdaptation;
import io.trino.plugin.jdbc.MergeJdbcPageSource.MergedRowAdaptation;
import io.trino.plugin.jdbc.MergeJdbcPageSource.SourceColumn;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.jdbc.DefaultJdbcMetadata.MERGE_ROW_ID;
import static io.trino.plugin.jdbc.RetryingModule.retry;
import static java.util.Objects.requireNonNull;

/**
 * ClickHouse page source provider that produces the {@code $merge_row_id} column for MERGE.
 * <p>
 * The default {@link io.trino.plugin.jdbc.JdbcPageSourceProvider} builds the merge row id as a row of the table's
 * primary-key columns. ClickHouse exposes no JDBC primary keys, so that path would build a zero-field row block which
 * mismatches the row id {@code RowType} declared by {@link ClickHouseMetadata#getMergeRowIdColumnHandle} and crashes
 * the merge processor. This provider instead builds the merge row id from <em>all</em> table columns, matching that
 * {@code RowType} field-for-field. The row id values are never read by {@link ClickHouseMergeSink} (which only inserts);
 * they only need a shape consistent with the declared type so the engine's merge blocks line up.
 */
public class ClickHousePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final JdbcClient jdbcClient;
    private final ExecutorService executor;
    private final RetryPolicy<Object> policy;

    @Inject
    public ClickHousePageSourceProvider(JdbcClient jdbcClient, @ForJdbcClient ExecutorService executor, RetryPolicy<Object> policy)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.policy = requireNonNull(policy, "policy is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        JdbcSplit jdbcSplit = (JdbcSplit) split;
        List<JdbcColumnHandle> jdbcColumns = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        Optional<JdbcColumnHandle> mergeRowId = jdbcColumns.stream()
                .filter(column -> column.getColumnName().equalsIgnoreCase(MERGE_ROW_ID))
                .collect(toOptional());
        if (mergeRowId.isEmpty()) {
            return new JdbcPageSource(
                    jdbcClient,
                    executor,
                    session,
                    jdbcSplit,
                    tableHandle.intersectedWithConstraint(jdbcSplit.getDynamicFilter().transformKeys(ColumnHandle.class::cast)),
                    jdbcColumns);
        }

        return createMergePageSource(session, jdbcSplit, jdbcColumns, tableHandle, mergeRowId.get());
    }

    private MergeJdbcPageSource createMergePageSource(
            ConnectorSession session,
            JdbcSplit jdbcSplit,
            List<JdbcColumnHandle> columns,
            JdbcTableHandle tableHandle,
            JdbcColumnHandle mergeRowId)
    {
        // The merge row id is a row of all table columns (see ClickHouseMetadata#getMergeRowIdColumnHandle), so the
        // scan is just the full set of table columns and the row id is assembled from every scan channel.
        List<JdbcColumnHandle> scanColumns = jdbcClient.getColumns(
                session,
                tableHandle.getRequiredNamedRelation().getSchemaTableName(),
                tableHandle.getRequiredNamedRelation().getRemoteTableName());
        List<Integer> allChannels = IntStream.range(0, scanColumns.size()).boxed().collect(toImmutableList());

        ImmutableList.Builder<ColumnAdaptation> columnAdaptationsBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columns) {
            if (columnHandle.equals(mergeRowId)) {
                columnAdaptationsBuilder.add(new MergedRowAdaptation(allChannels));
            }
            else {
                columnAdaptationsBuilder.add(new SourceColumn(scanColumns.indexOf(columnHandle)));
            }
        }

        JdbcTableHandle newTableHandle = new JdbcTableHandle(
                tableHandle.getRelationHandle(),
                tableHandle.getConstraint(),
                tableHandle.getConstraintExpressions(),
                tableHandle.getSortOrder(),
                tableHandle.getLimit(),
                Optional.of(scanColumns),
                tableHandle.getOtherReferencedTables(),
                tableHandle.getNextSyntheticColumnId(),
                tableHandle.getAuthorization(),
                tableHandle.getUpdateAssignments());
        return new MergeJdbcPageSource(
                createPageSource(session, jdbcSplit, newTableHandle, scanColumns),
                columnAdaptationsBuilder.build());
    }

    private JdbcPageSource createPageSource(
            ConnectorSession session,
            JdbcSplit jdbcSplit,
            BaseJdbcConnectorTableHandle table,
            List<JdbcColumnHandle> columnHandles)
    {
        return retry(policy, () -> new JdbcPageSource(jdbcClient, executor, session, jdbcSplit, table, columnHandles));
    }
}
