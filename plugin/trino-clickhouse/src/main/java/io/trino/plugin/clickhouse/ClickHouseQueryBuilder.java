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
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcRelationHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.plugin.clickhouse.ClickHouseSessionProperties.isUseFinal;

/**
 * Adds ClickHouse's {@code FINAL} modifier to read queries when the {@code use_final} session property is enabled, so
 * MergeTree-family engines like ReplacingMergeTree return deduplicated rows even before the background merge has
 * collapsed re-inserted versions (the diamond serving layer relies on this to read consistent upserted data).
 * <p>
 * {@code FINAL} is appended only for a plain named-table scan ({@code FROM "db"."tbl" FINAL}) whose engine actually
 * supports FINAL (the collapsing MergeTree variants — see {@link ClickHouseClient#tableSupportsFinal}); plain
 * {@code MergeTree}, Log, etc. are left untouched because ClickHouse rejects FINAL on them. Pushed-down subqueries
 * (joins, aggregations) alias their FROM as {@code (...) o} and are also left untouched. Only SELECTs go through
 * {@link #getFrom}; DELETE/UPDATE build their relation via {@code getRelation} and are unaffected.
 */
public class ClickHouseQueryBuilder
        extends DefaultQueryBuilder
{
    // getFrom() carries no ConnectorSession, so the per-query session read in prepareSelectQuery is passed to getFrom
    // across the same-thread call via this holder (used to check use_final and to look up the table engine).
    private final ThreadLocal<ConnectorSession> currentSession = new ThreadLocal<>();

    @Inject
    public ClickHouseQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    public PreparedQuery prepareSelectQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcRelationHandle baseRelation,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ParameterizedExpression> additionalPredicate)
    {
        currentSession.set(session);
        try {
            return super.prepareSelectQuery(client, session, connection, baseRelation, groupingSets, columns, columnExpressions, tupleDomain, additionalPredicate);
        }
        finally {
            currentSession.remove();
        }
    }

    @Override
    protected String getFrom(JdbcClient client, JdbcRelationHandle baseRelation, Consumer<QueryParameter> accumulator)
    {
        String from = super.getFrom(client, baseRelation, accumulator);
        ConnectorSession session = currentSession.get();
        if (session != null
                && isUseFinal(session)
                && baseRelation instanceof JdbcNamedRelationHandle namedRelation
                && client instanceof ClickHouseClient clickHouseClient
                && clickHouseClient.tableSupportsFinal(session, namedRelation.getRemoteTableName())) {
            return from + " FINAL";
        }
        return from;
    }
}
