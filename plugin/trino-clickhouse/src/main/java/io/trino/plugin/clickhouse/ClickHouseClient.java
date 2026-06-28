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

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule.RewriteContext;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.clickhouse.expression.RewriteLike;
import io.trino.plugin.clickhouse.expression.RewriteStringComparison;
import io.trino.plugin.clickhouse.expression.RewriteStringIn;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.clickhouse.data.ClickHouseUtils.escape;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.clickhouse.ClickHouseSessionProperties.isMapStringAsVarchar;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ORDER_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PARTITION_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.SAMPLE_BY_PROPERTY;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.DATETIME;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.DATETIME64;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT16;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT32;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT64;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT8;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.arraycopy;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Splitter TABLE_PROPERTY_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private static final DecimalType UINT64_TYPE = createDecimalType(20, 0);

    // ClickHouse DateTime64 supports a fractional scale from 0 (seconds) up to 9 (nanoseconds).
    private static final int MAX_CLICKHOUSE_DATETIME64_SCALE = 9;

    // An empty character means that the table doesn't have a comment in ClickHouse
    private static final String NO_COMMENT = "";

    public static final int DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 1_000;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final Type uuidType;
    private final Type ipAddressType;
    private final Type jsonType;
    private final TypeOperators typeOperators;
    private final AtomicReference<ClickHouseVersion> clickHouseVersion = new AtomicReference<>();

    @Inject
    public ClickHouseClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.ipAddressType = typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS));
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.typeOperators = typeManager.getTypeOperators();
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteStringComparison())
                .add(new RewriteStringIn())
                .add(new RewriteLike())
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(ClickHouseClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementCorr())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.column().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // Clickhouse maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT");
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        else if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // ClickHouse does not support `create table tbl as select * from tbl2 where 0=1`
        // ClickHouse supports the following two methods to copy schema
        // 1. create table tbl as tbl2
        // 2. create table tbl1 ENGINE=<engine> as select * from tbl2
        String sql = format(
                "CREATE TABLE %s AS %s ",
                quoted(null, schemaName, newTableName),
                quoted(null, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        // for Clickhouse, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in ClickHouse
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        ClickHouseEngineType engine = ClickHouseTableProperties.getEngine(tableProperties);
        tableOptions.add("ENGINE = " + engine.getEngineType());
        if (engine.isMergeTreeFamily() && formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).isEmpty()) {
            // https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#order_by
            tableOptions.add("ORDER BY tuple()");
        }
        formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).ifPresent(value -> tableOptions.add("ORDER BY " + value));
        formatProperty(ClickHouseTableProperties.getPrimaryKey(tableProperties)).ifPresent(value -> tableOptions.add("PRIMARY KEY " + value));
        formatProperty(ClickHouseTableProperties.getPartitionBy(tableProperties)).ifPresent(value -> tableOptions.add("PARTITION BY " + value));
        ClickHouseTableProperties.getSampleBy(tableProperties).ifPresent(value -> tableOptions.add("SAMPLE BY " + quoted(value)));
        tableMetadata.getComment().ifPresent(comment -> tableOptions.add(format("COMMENT %s", clickhouseVarcharLiteral(comment))));

        return ImmutableList.of(format("CREATE TABLE %s (%s) %s", quoted(remoteTableName), join(", ", columns), join(" ", tableOptions.build())));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = connection.prepareStatement("" +
                        "SELECT engine, sorting_key, partition_key, primary_key, sampling_key " +
                        "FROM system.tables " +
                        "WHERE database = ? AND name = ?")) {
            statement.setString(1, tableHandle.asPlainTable().getRemoteTableName().getCatalogName().orElse(null));
            statement.setString(2, tableHandle.asPlainTable().getRemoteTableName().getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
                while (resultSet.next()) {
                    String engine = resultSet.getString("engine");
                    if (!isNullOrEmpty(engine)) {
                        // Don't throw an exception because many table engines aren't supported in ClickHouseEngineType
                        Optional<ClickHouseEngineType> engineType = Enums.getIfPresent(ClickHouseEngineType.class, engine.toUpperCase(ENGLISH)).toJavaUtil();
                        engineType.ifPresent(type -> properties.put(ENGINE_PROPERTY, type));
                    }
                    String sortingKey = resultSet.getString("sorting_key");
                    if (!isNullOrEmpty(sortingKey)) {
                        properties.put(ORDER_BY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(sortingKey));
                    }
                    String partitionKey = resultSet.getString("partition_key");
                    if (!isNullOrEmpty(partitionKey)) {
                        properties.put(PARTITION_BY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(partitionKey));
                    }
                    String primaryKey = resultSet.getString("primary_key");
                    if (!isNullOrEmpty(primaryKey)) {
                        properties.put(PRIMARY_KEY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(primaryKey));
                    }
                    String samplingKey = resultSet.getString("sampling_key");
                    if (!isNullOrEmpty(samplingKey)) {
                        properties.put(SAMPLE_BY_PROPERTY, samplingKey);
                    }
                }
                return properties.buildOrThrow();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    /**
     * Whether the given table's engine supports the {@code FINAL} read modifier. FINAL is valid for the MergeTree
     * variants that collapse rows on merge (Replacing/Aggregating/Summing/Collapsing/VersionedCollapsing, including
     * their Replicated/Shared forms) but NOT for a plain {@code MergeTree} (ClickHouse rejects it with
     * "Storage MergeTree doesn't support FINAL"). Used by {@link ClickHouseQueryBuilder} to add FINAL only where it is
     * legal when {@code use_final} is enabled, so the catalog can still read plain-MergeTree (and Log, etc.) tables.
     */
    public boolean tableSupportsFinal(ConnectorSession session, RemoteTableName remoteTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT engine FROM system.tables WHERE database = ? AND name = ?")) {
            statement.setString(1, remoteTableName.getCatalogName().orElse(null));
            statement.setString(2, remoteTableName.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return false;
                }
                return engineSupportsFinal(resultSet.getString("engine"));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static boolean engineSupportsFinal(String engine)
    {
        if (isNullOrEmpty(engine)) {
            return false;
        }
        // Strip any Replicated/Shared prefix (e.g. ReplicatedReplacingMergeTree) before classifying.
        String normalized = engine
                .replaceFirst("^Replicated", "")
                .replaceFirst("^Shared", "");
        // Plain MergeTree does not support FINAL; the collapsing variants do (they end in "MergeTree" but are not "MergeTree").
        return normalized.endsWith("MergeTree") && !normalized.equals("MergeTree");
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> nullableProperties)
    {
        // TODO: Support other table properties
        checkArgument(nullableProperties.size() == 1 && nullableProperties.containsKey(SAMPLE_BY_PROPERTY), "Only support setting 'sample_by' property");
        // TODO: Support sampling key removal when we support a newer version of ClickHouse. See https://github.com/ClickHouse/ClickHouse/pull/30180.
        checkArgument(nullableProperties.values().stream().noneMatch(Optional::isEmpty), "Setting a property to null is not supported");

        Map<String, Object> properties = nullableProperties.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow()));

        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        ClickHouseTableProperties.getSampleBy(properties).ifPresent(value -> tableOptions.add("SAMPLE BY " + quoted(value)));

        try (Connection connection = connectionFactory.openConnection(session)) {
            String sql = format(
                    "ALTER TABLE %s MODIFY %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    join(" ", tableOptions.build()));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ");
        String dataType = toWriteMapping(session, column.getType()).getDataType();
        // ClickHouse does not allow Array/Map to be wrapped in Nullable (they carry their own absence semantics:
        // an empty container), so such columns are emitted without the Nullable wrapper.
        if (column.isNullable() && !isClickHouseNonNullableContainer(column.getType())) {
            // set column nullable property explicitly
            sb.append("Nullable(").append(dataType).append(")");
        }
        else {
            // By default, the clickhouse column is not allowed to be null
            sb.append(dataType);
        }
        if (column.getComment() != null) {
            sb.append(format(" COMMENT %s", clickhouseVarcharLiteral(column.getComment())));
        }
        return sb.toString();
    }

    private static boolean isClickHouseNonNullableContainer(Type type)
    {
        // ClickHouse rejects Nullable(Array(...)) and Nullable(Map(...)); these types represent absence as an empty container.
        return type instanceof ArrayType || type instanceof MapType;
    }

    private String clickHouseElementDataType(ConnectorSession session, Type elementType)
    {
        // Trino array elements and map values are nullable. Wrap the ClickHouse type in Nullable when it can be (scalars);
        // nested Array/Map cannot be inside Nullable, so they are emitted bare.
        String dataType = toWriteMapping(session, elementType).getDataType();
        if (isClickHouseNonNullableContainer(elementType)) {
            return dataType;
        }
        return format("Nullable(%s)", dataType);
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "CREATE DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        // ClickHouse always deletes all tables inside the database https://clickhouse.com/docs/en/sql-reference/statements/drop
        if (!cascade) {
            try (ResultSet tables = getTables(connection, Optional.of(remoteSchemaName), Optional.empty())) {
                if (tables.next()) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(remoteSchemaName));
                }
            }
        }
        execute(session, connection, "DROP DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    protected void renameSchema(ConnectorSession session, Connection connection, String remoteSchemaName, String newRemoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "RENAME DATABASE " + quoted(remoteSchemaName) + " TO " + quoted(newRemoteSchemaName));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        switch (position) {
            case ColumnPosition.First _ -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with FIRST clause");
            case ColumnPosition.After _ -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with AFTER clause");
            case ColumnPosition.Last _ -> addColumn(session, handle.asPlainTable().getRemoteTableName(), column);
        }
    }

    private void addColumn(ConnectorSession session, RemoteTableName table, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getName());
            String sql = format(
                    "ALTER TABLE %s ADD COLUMN %s",
                    quoted(table),
                    getColumnDefinitionSql(session, column, remoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s MODIFY COMMENT %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                clickhouseVarcharLiteral(comment.orElse(NO_COMMENT)));
        execute(session, sql);
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT COLUMN %s %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                clickhouseVarcharLiteral(comment.orElse("")));
        execute(session, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    private static String clickhouseVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + escape(value, '\'') + "'";
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.empty();
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        execute(session, connection, format("RENAME TABLE %s TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(catalogName, newRemoteSchemaName, newRemoteTableName)));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        // Modern ClickHouse (>= 22.8) executes a standard "DELETE FROM <table> WHERE <predicate>" as a synchronous
        // lightweight delete on MergeTree-family engines (rejected on Log, so deleted-from tables must be MergeTree).
        // Two ClickHouse-specific adaptations vs. the base JDBC delete:
        //  1. ClickHouse does not report the affected-row count (executeUpdate always returns 0), so the count is
        //     obtained with a matching "SELECT count(*) ... WHERE <predicate>" evaluated before the delete.
        //  2. ClickHouse requires a WHERE clause; an unconstrained full-table delete is executed as TRUNCATE.
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        checkArgument(handle.getUpdateAssignments().isEmpty(), "Unable to delete when update assignments are set: %s", handle);

        if (handle.getConstraint().isAll() && handle.getConstraintExpressions().isEmpty()) {
            try (Connection connection = connectionFactory.openConnection(session)) {
                long rowCount = countRows(session, connection, handle);
                truncateTable(session, handle);
                return OptionalLong.of(rowCount);
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            // ClickHouse returns 0 affected rows for a delete, so count the matching rows with the same predicate first.
            long rowCount = countRows(session, connection, preparedQuery);
            try (PreparedStatement statement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                statement.executeUpdate();
            }
            return OptionalLong.of(rowCount);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private long countRows(ConnectorSession session, Connection connection, JdbcTableHandle handle)
            throws SQLException
    {
        PreparedQuery deleteQuery = queryBuilder.prepareDeleteQuery(
                this,
                session,
                connection,
                handle.getRequiredNamedRelation(),
                handle.getConstraint(),
                getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
        return countRows(session, connection, deleteQuery);
    }

    private long countRows(ConnectorSession session, Connection connection, PreparedQuery deleteQuery)
            throws SQLException
    {
        // Rewrite "DELETE FROM <relation> [WHERE ...]" into "SELECT count(*) FROM <relation> [WHERE ...]", reusing the
        // exact relation, predicate and bound parameters that the delete will apply.
        PreparedQuery countQuery = deleteQuery.transformQuery(sql -> "SELECT count(*) FROM " + sql.substring("DELETE FROM ".length()));
        try (PreparedStatement statement = queryBuilder.prepareStatement(this, session, connection, countQuery, Optional.empty());
                ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public boolean supportsMerge()
    {
        // MERGE is implemented as INSERT-only against a ReplacingMergeTree target (see ClickHouseMergeSink): every
        // changed or new row is re-inserted and ClickHouse collapses duplicates by ORDER BY key on background merge,
        // so the diamond serving layer never performs row-level UPDATE/DELETE mutations. The target table must be
        // ReplacingMergeTree with its ORDER BY equal to the merge keys for dedup to be correct; that is configured by
        // the dbt model (engine='ReplacingMergeTree', order_by=<unique_key>).
        return true;
    }

    @Override
    public JdbcMergeTableHandle beginMerge(
            ConnectorSession session,
            JdbcTableHandle handle,
            Map<Integer, Collection<ColumnHandle>> updateColumnHandles,
            Consumer<Runnable> rollbackActionCollector,
            RetryMode retryMode)
    {
        // Unlike the base JDBC merge (which deletes/updates rows by primary key) this implementation only ever inserts,
        // so it needs neither primary-key metadata (ClickHouse does not report any) nor delete/update output handles.
        // It pairs with RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW (see ClickHouseMetadata): the engine turns every
        // matched-update into a delete-row + a full insert-row, and ClickHouseMergeSink keeps only the insert rows.
        SchemaTableName schemaTableName = handle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();
        List<JdbcColumnHandle> columns = getColumns(session, schemaTableName, remoteTableName);

        JdbcTableHandle plainTable = new JdbcTableHandle(schemaTableName, remoteTableName, Optional.empty());
        JdbcOutputTableHandle outputTableHandle = beginInsertTable(session, plainTable, columns);
        rollbackActionCollector.accept(() -> rollbackTemporaryTableCreation(session, outputTableHandle));

        return new JdbcMergeTableHandle(
                handle,
                outputTableHandle,
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableList.of(),
                columns,
                ImmutableMap.of());
    }

    @Override
    public void finishMerge(ConnectorSession session, JdbcMergeTableHandle handle, Set<Long> pageSinkIds)
    {
        // The insert rows were appended to the target table directly by ClickHouseMergeSink; there is no temporary
        // table to swap in, so completing the underlying insert is all that is required.
        finishInsertTable(session, handle.getOutputTableHandle(), pageSinkIds);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        ClickHouseVersion version = getClickHouseServerVersion(session);
        ClickHouseColumn column = ClickHouseColumn.of("", jdbcTypeName);
        ClickHouseDataType columnDataType = column.getDataType();
        switch (columnDataType) {
            case Bool:
                return Optional.of(booleanColumnMapping());
            case UInt8:
                return Optional.of(ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, uInt8WriteFunction(getClickHouseServerVersion(session))));
            case UInt16:
                return Optional.of(ColumnMapping.longMapping(INTEGER, ResultSet::getInt, uInt16WriteFunction(getClickHouseServerVersion(session))));
            case UInt32:
                return Optional.of(ColumnMapping.longMapping(BIGINT, ResultSet::getLong, uInt32WriteFunction(getClickHouseServerVersion(session))));
            case UInt64:
                return Optional.of(ColumnMapping.objectMapping(
                        UINT64_TYPE,
                        longDecimalReadFunction(UINT64_TYPE, UNNECESSARY),
                        uInt64WriteFunction(getClickHouseServerVersion(session))));
            case IPv4:
            case IPv6:
                return Optional.of(ipAddressColumnMapping(column.getOriginalTypeName()));
            case Enum8:
            case Enum16:
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        varcharReadFunction(createUnboundedVarcharType()),
                        varcharWriteFunction(),
                        // TODO (https://github.com/trinodb/trino/issues/7100) Currently pushdown would not work and may require a custom bind expression
                        DISABLE_PUSHDOWN));

            case FixedString: // FixedString(n)
            case String:
                if (isMapStringAsVarchar(session)) {
                    return Optional.of(ColumnMapping.sliceMapping(
                            createUnboundedVarcharType(),
                            varcharReadFunction(createUnboundedVarcharType()),
                            varcharWriteFunction(),
                            FULL_PUSHDOWN));
                }
                return Optional.of(varbinaryColumnMapping());
            case UUID:
                return Optional.of(uuidColumnMapping());
            case Array: {
                Optional<ColumnMapping> arrayMapping = arrayColumnMapping(session, connection, column);
                if (arrayMapping.isPresent()) {
                    return arrayMapping;
                }
                break;
            }
            case Map: {
                Optional<ColumnMapping> mapMapping = mapColumnMapping(session, connection, column);
                if (mapMapping.isPresent()) {
                    return mapMapping;
                }
                break;
            }
            default:
                // no-op
        }

        switch (typeHandle.jdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
            case Types.REAL:
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize();

                ColumnMapping decimalColumnMapping;
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = Math.min(decimalDigits, getDecimalDefaultScale(session));
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session));
                }
                else {
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)));
                }
                return Optional.of(ColumnMapping.mapping(
                        decimalColumnMapping.getType(),
                        decimalColumnMapping.getReadFunction(),
                        decimalColumnMapping.getWriteFunction(),
                        // TODO (https://github.com/trinodb/trino/issues/7100) fix, enable and test decimal pushdown
                        DISABLE_PUSHDOWN));

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate(version));

            case Types.TIMESTAMP:
                if (columnDataType == ClickHouseDataType.DateTime) {
                    // ClickHouse DateTime does not have sub-second precision
                    verify(typeHandle.requiredDecimalDigits() == 0, "Expected 0 as timestamp precision, but got %s", typeHandle.requiredDecimalDigits());
                    return Optional.of(ColumnMapping.longMapping(
                            TIMESTAMP_SECONDS,
                            timestampReadFunction(TIMESTAMP_SECONDS),
                            timestampSecondsWriteFunction(version)));
                }
                // ClickHouse DateTime64(scale) preserves sub-second precision; map it to TIMESTAMP with the same
                // precision. The write function validates the value range (INSERT into an existing table uses the read
                // mapping's write function, so range checking must live here, not only in toWriteMapping).
                int timestampPrecision = column.getScale();
                verify(timestampPrecision >= 0 && timestampPrecision <= MAX_CLICKHOUSE_DATETIME64_SCALE, "Unexpected DateTime64 scale: %s", timestampPrecision);
                TimestampType timestampTrinoType = createTimestampType(timestampPrecision);
                if (timestampTrinoType.isShort()) {
                    return Optional.of(ColumnMapping.longMapping(
                            timestampTrinoType,
                            timestampReadFunction(timestampTrinoType),
                            shortDateTime64WriteFunction(version)));
                }
                return Optional.of(ColumnMapping.objectMapping(
                        timestampTrinoType,
                        longTimestampReadFunction(timestampTrinoType),
                        longDateTime64WriteFunction(version, timestampPrecision)));

            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (columnDataType == ClickHouseDataType.DateTime) {
                    // ClickHouse DateTime does not have sub-second precision
                    verify(typeHandle.requiredDecimalDigits() == 0, "Expected 0 as timestamp with time zone precision, but got %s", typeHandle.requiredDecimalDigits());
                    return Optional.of(ColumnMapping.longMapping(
                            TIMESTAMP_TZ_SECONDS,
                            shortTimestampWithTimeZoneReadFunction(),
                            shortTimestampWithTimeZoneWriteFunction(version, column.getTimeZone())));
                }
                if (columnDataType == ClickHouseDataType.DateTime64) {
                    // DateTime64(scale, tz) preserves sub-second precision; map it to TIMESTAMP(scale) WITH TIME ZONE.
                    int tzPrecision = column.getScale();
                    verify(tzPrecision >= 0 && tzPrecision <= MAX_CLICKHOUSE_DATETIME64_SCALE, "Unexpected DateTime64 scale: %s", tzPrecision);
                    TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(tzPrecision);
                    if (trinoType.isShort()) {
                        return Optional.of(ColumnMapping.longMapping(
                                trinoType,
                                shortTimestampWithTimeZoneReadFunction(),
                                shortDateTime64WithTimeZoneWriteFunction()));
                    }
                    return Optional.of(ColumnMapping.objectMapping(
                            trinoType,
                            longTimestampWithTimeZoneReadFunction(),
                            longTimestampWithTimeZoneWriteFunction()));
                }
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private Optional<ColumnMapping> arrayColumnMapping(ConnectorSession session, Connection connection, ClickHouseColumn arrayColumn)
    {
        ClickHouseColumn elementColumn = arrayColumn.getArrayBaseColumn();
        // ClickHouseDataType.getVendorTypeNumber() is a ClickHouse-internal code, not a java.sql.Types value;
        // JdbcUtils.convertToSqlType maps the element type to the JDBC type the recursive mapping expects.
        JdbcTypeHandle elementTypeHandle = new JdbcTypeHandle(
                JdbcUtils.convertToSqlType(elementColumn.getDataType()).getVendorTypeNumber(),
                Optional.of(elementColumn.getOriginalTypeName()),
                Optional.of(elementColumn.getPrecision()),
                Optional.of(elementColumn.getScale()),
                Optional.empty(),
                Optional.empty());
        return toColumnMapping(session, connection, elementTypeHandle)
                .map(elementMapping -> {
                    ArrayType arrayType = new ArrayType(elementMapping.getType());
                    return ColumnMapping.objectMapping(
                            arrayType,
                            arrayReadFunction(arrayType.getElementType()),
                            arrayWriteFunction(arrayType.getElementType()));
                });
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType)
    {
        // The clickhouse-jdbc 0.9.x ClickHouseArray does not implement java.sql.Array.getResultSet(), so the values are
        // read from the Java array returned by getArray(). That array may be a primitive array (e.g. long[] for
        // Array(Int64)) or an Object[] (e.g. for nullable or String elements), so it is iterated reflectively.
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Object elements = resultSet.getArray(columnIndex).getArray();
            int length = java.lang.reflect.Array.getLength(elements);
            BlockBuilder builder = elementType.createBlockBuilder(null, length);
            for (int i = 0; i < length; i++) {
                appendArrayElement(elementType, builder, java.lang.reflect.Array.get(elements, i));
            }
            return builder.build();
        });
    }

    private static void appendArrayElement(Type elementType, BlockBuilder builder, Object element)
    {
        if (element == null) {
            builder.appendNull();
        }
        else {
            writeNativeValue(elementType, builder, toTrinoArrayElement(elementType, element));
        }
    }

    private static Object toTrinoArrayElement(Type elementType, Object element)
    {
        if (elementType instanceof VarcharType || elementType instanceof CharType) {
            return element instanceof byte[] bytes ? wrappedBuffer(bytes) : utf8Slice((String) element);
        }
        if (elementType instanceof VarbinaryType) {
            return wrappedBuffer((byte[]) element);
        }
        if (elementType == REAL) {
            return (long) floatToRawIntBits(((Number) element).floatValue());
        }
        if (elementType == TINYINT || elementType == SMALLINT || elementType == INTEGER || elementType == BIGINT) {
            return ((Number) element).longValue();
        }
        if (elementType == BOOLEAN || elementType == DOUBLE) {
            return element;
        }
        // Decimal, timestamp, date and nested array/map elements are already in their native representation, written via writeNativeValue.
        return element;
    }

    private static ObjectWriteFunction arrayWriteFunction(Type elementType)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            List<Object> values = new ArrayList<>(block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add(elementType.getObjectValue(block, position));
            }
            statement.setObject(index, values);
        });
    }

    private Optional<ColumnMapping> mapColumnMapping(ConnectorSession session, Connection connection, ClickHouseColumn mapColumn)
    {
        ClickHouseColumn keyColumn = mapColumn.getKeyInfo();
        ClickHouseColumn valueColumn = mapColumn.getValueInfo();
        Optional<ColumnMapping> keyMapping = toColumnMapping(session, connection, mapElementTypeHandle(keyColumn));
        Optional<ColumnMapping> valueMapping = toColumnMapping(session, connection, mapElementTypeHandle(valueColumn));
        if (keyMapping.isEmpty() || valueMapping.isEmpty()) {
            return Optional.empty();
        }
        MapType mapType = new MapType(keyMapping.get().getType(), valueMapping.get().getType(), typeOperators);
        return Optional.of(ColumnMapping.objectMapping(mapType, mapReadFunction(mapType), mapWriteFunction(mapType)));
    }

    private static JdbcTypeHandle mapElementTypeHandle(ClickHouseColumn column)
    {
        // See arrayColumnMapping: convertToSqlType maps the ClickHouse type to the java.sql.Types the recursion expects.
        return new JdbcTypeHandle(
                JdbcUtils.convertToSqlType(column.getDataType()).getVendorTypeNumber(),
                Optional.of(column.getOriginalTypeName()),
                Optional.of(column.getPrecision()),
                Optional.of(column.getScale()),
                Optional.empty(),
                Optional.empty());
    }

    private static ObjectReadFunction mapReadFunction(MapType mapType)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        // The clickhouse-jdbc driver returns a Map column as a java.util.Map; build the Trino map block from its entries.
        return ObjectReadFunction.of(SqlMap.class, (resultSet, columnIndex) -> {
            Map<?, ?> map = (Map<?, ?>) resultSet.getObject(columnIndex);
            BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
            ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) -> {
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    writeNativeValue(keyType, keyBuilder, toTrinoMapElement(keyType, entry.getKey()));
                    if (entry.getValue() == null) {
                        valueBuilder.appendNull();
                    }
                    else {
                        writeNativeValue(valueType, valueBuilder, toTrinoMapElement(valueType, entry.getValue()));
                    }
                }
            });
            return mapType.getObject(blockBuilder.build(), 0);
        });
    }

    private static Object toTrinoMapElement(Type type, Object value)
    {
        if (type instanceof ArrayType arrayType) {
            // Nested array values (e.g. multimap Map(K, Array(V))) come back from the driver either as a primitive
            // array (e.g. long[]) or a java.util.List, depending on the element type; handle both reflectively.
            Type elementType = arrayType.getElementType();
            if (value instanceof List<?> list) {
                BlockBuilder builder = elementType.createBlockBuilder(null, list.size());
                for (Object element : list) {
                    appendArrayElement(elementType, builder, element);
                }
                return builder.build();
            }
            int length = java.lang.reflect.Array.getLength(value);
            BlockBuilder builder = elementType.createBlockBuilder(null, length);
            for (int i = 0; i < length; i++) {
                appendArrayElement(elementType, builder, java.lang.reflect.Array.get(value, i));
            }
            return builder.build();
        }
        return toTrinoArrayElement(type, value);
    }

    private static ObjectWriteFunction mapWriteFunction(MapType mapType)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        return ObjectWriteFunction.of(SqlMap.class, (statement, index, sqlMap) -> {
            int rawOffset = sqlMap.getRawOffset();
            Block keyBlock = sqlMap.getRawKeyBlock();
            Block valueBlock = sqlMap.getRawValueBlock();
            Map<Object, Object> map = new LinkedHashMap<>(sqlMap.getSize());
            for (int i = 0; i < sqlMap.getSize(); i++) {
                map.put(keyType.getObjectValue(keyBlock, rawOffset + i), valueType.getObjectValue(valueBlock, rawOffset + i));
            }
            statement.setObject(index, map);
        });
    }

    private static SliceWriteFunction jsonWriteFunction()
    {
        // Trino JSON is canonical JSON text; it is written verbatim into a ClickHouse String column.
        return (statement, index, value) -> statement.setString(index, value.toStringUtf8());
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("Bool", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("Int8", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("Int16", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("Int32", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("Int64", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("Float32", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Float64", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            // The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            // Strings of an arbitrary length. The length is not limited
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("Date", dateWriteFunctionUsingLocalDate(getClickHouseServerVersion(session)));
        }
        if (type == TIMESTAMP_SECONDS) {
            return WriteMapping.longMapping("DateTime", timestampSecondsWriteFunction(getClickHouseServerVersion(session)));
        }
        if (type instanceof TimestampType timestampType) {
            verify(timestampType.getPrecision() > 0 && timestampType.getPrecision() <= MAX_CLICKHOUSE_DATETIME64_SCALE, "Unexpected timestamp precision: %s", timestampType.getPrecision());
            String dataType = format("DateTime64(%s)", timestampType.getPrecision());
            if (timestampType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDateTime64WriteFunction(getClickHouseServerVersion(session)));
            }
            return WriteMapping.objectMapping(dataType, longDateTime64WriteFunction(getClickHouseServerVersion(session), timestampType.getPrecision()));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            // The instant is stored in a DateTime64(p, 'UTC') column. NOTE: ClickHouse's DateTime64 time zone is a
            // per-COLUMN display setting over a UTC instant; it has no per-ROW zone like Trino's "timestamp with time
            // zone". So the absolute instant is preserved exactly, but the original per-row offset is normalized to UTC
            // on read-back (e.g. '... +05:45' reads back as '... +00:00'). This is a ClickHouse limitation, acceptable
            // for the diamond/serving use case which only needs the instant. Precision 0 keeps the existing DateTime path.
            int precision = timestampWithTimeZoneType.getPrecision();
            verify(precision >= 0 && precision <= MAX_CLICKHOUSE_DATETIME64_SCALE, "Unexpected timestamp with time zone precision: %s", precision);
            if (precision == 0) {
                return WriteMapping.longMapping("DateTime('UTC')", shortTimestampWithTimeZoneWriteFunction(getClickHouseServerVersion(session), TimeZone.getTimeZone(UTC)));
            }
            String dataType = format("DateTime64(%s, 'UTC')", precision);
            if (timestampWithTimeZoneType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDateTime64WithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("UUID", uuidWriteFunction());
        }
        if (type.equals(jsonType)) {
            // Trino JSON is serialized text. It is stored in a ClickHouse String column (queryable via JSONExtract*()).
            // ClickHouse's native JSON type is NOT used: the connector inserts via RowBinary, and clickhouse-jdbc 0.9.8
            // cannot binary-encode a String into a native JSON column (CANNOT_READ_ALL_DATA). A String column accepts
            // the JSON text bytes over RowBinary and keeps diamond models as pure SELECT *.
            return WriteMapping.sliceMapping("String", jsonWriteFunction());
        }
        if (type instanceof ArrayType arrayType) {
            // Trino array elements are nullable, so the ClickHouse element type is wrapped in Nullable when it can be
            // (scalars). Nested containers (Array/Map) cannot be inside Nullable and are emitted bare.
            String elementDataType = clickHouseElementDataType(session, arrayType.getElementType());
            return WriteMapping.objectMapping(
                    format("Array(%s)", elementDataType),
                    arrayWriteFunction(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            // ClickHouse Map keys must NOT be Nullable; values are nullable in Trino, so they are wrapped in Nullable
            // when possible (scalars) and left bare for nested containers (e.g. Array, for multimaps).
            String keyDataType = toWriteMapping(session, mapType.getKeyType()).getDataType();
            String valueDataType = clickHouseElementDataType(session, mapType.getValueType());
            return WriteMapping.objectMapping(format("Map(%s, %s)", keyDataType, valueDataType), mapWriteFunction(mapType));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    private ClickHouseVersion getClickHouseServerVersion(ConnectorSession session)
    {
        return clickHouseVersion.updateAndGet(current -> {
            if (current != null) {
                return current;
            }

            try (Connection connection = connectionFactory.openConnection(session);
                    PreparedStatement statement = connection.prepareStatement("SELECT version()");
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    current = ClickHouseVersion.of(resultSet.getString(1));
                }
                return current;
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        });
    }

    /**
     * format property to match ClickHouse create table statement
     *
     * @param prop property will be formatted
     * @return formatted property
     */
    private Optional<String> formatProperty(List<String> prop)
    {
        if (prop == null || prop.isEmpty()) {
            return Optional.empty();
        }
        if (prop.size() == 1) {
            // only one column
            return Optional.of(quoted(prop.get(0)));
        }
        // include more than one column
        return Optional.of(prop.stream().map(this::quoted).collect(Collectors.joining(",", "(", ")")));
    }

    private static LongWriteFunction uInt8WriteFunction(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            UINT8.validate(version, value);
            statement.setShort(index, Shorts.checkedCast(value));
        };
    }

    private static LongWriteFunction uInt16WriteFunction(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            UINT16.validate(version, value);
            statement.setInt(index, toIntExact(value));
        };
    }

    private static LongWriteFunction uInt32WriteFunction(ClickHouseVersion version)
    {
        return (preparedStatement, parameterIndex, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            UINT32.validate(version, value);
            preparedStatement.setLong(parameterIndex, value);
        };
    }

    private static ObjectWriteFunction uInt64WriteFunction(ClickHouseVersion version)
    {
        return ObjectWriteFunction.of(
                Int128.class,
                (statement, index, value) -> {
                    BigInteger unscaledValue = value.toBigInteger();
                    BigDecimal bigDecimal = new BigDecimal(unscaledValue, UINT64_TYPE.getScale(), new MathContext(UINT64_TYPE.getPrecision()));
                    // ClickHouse stores incorrect results when the values are out of supported range.
                    UINT64.validate(version, bigDecimal);
                    statement.setBigDecimal(index, bigDecimal);
                });
    }

    private static ColumnMapping dateColumnMappingUsingLocalDate(ClickHouseVersion version)
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                dateWriteFunctionUsingLocalDate(version));
    }

    private static LongWriteFunction dateWriteFunctionUsingLocalDate(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            LocalDate date = LocalDate.ofEpochDay(value);
            // Deny unsupported dates eagerly to prevent unexpected results. ClickHouse stores '1970-01-01' when the date is out of supported range.
            TrinoToClickHouseWriteChecker.DATE.validate(version, date);
            statement.setObject(index, date);
        };
    }

    private static LongWriteFunction timestampSecondsWriteFunction(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            long epochSecond = floorDiv(value, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(value, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            verify(nanoFraction == 0, "Nanos of second must be zero: '%s'", value);
            LocalDateTime timestamp = LocalDateTime.ofEpochSecond(epochSecond, 0, UTC);
            // ClickHouse stores incorrect results when the values are out of supported range.
            DATETIME.validate(version, timestamp);
            statement.setObject(index, timestamp);
        };
    }

    private static LongWriteFunction shortDateTime64WriteFunction(ClickHouseVersion version)
    {
        // Validate against the DateTime64 range (ClickHouse silently clamps out-of-range values), then delegate the bind.
        LongWriteFunction delegate = timestampWriteFunction(TIMESTAMP_MICROS);
        return (statement, index, value) -> {
            DATETIME64.validate(version, fromTrinoTimestamp(value));
            delegate.set(statement, index, value);
        };
    }

    private static ObjectWriteFunction longDateTime64WriteFunction(ClickHouseVersion version, int precision)
    {
        ObjectWriteFunction delegate = longTimestampWriteFunction(createTimestampType(precision), precision);
        return ObjectWriteFunction.of(LongTimestamp.class, (statement, index, value) -> {
            DATETIME64.validate(version, fromLongTrinoTimestamp(value, precision));
            delegate.set(statement, index, value);
        });
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            ZonedDateTime zonedDateTime = resultSet.getObject(columnIndex, ZonedDateTime.class);
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction(ClickHouseVersion version, TimeZone columnTimeZone)
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            // Clickhouse JDBC driver inserts datetime as string value as yyyy-MM-dd HH:mm:ss and zone from the Column metadata would be used.
            Instant instant = Instant.ofEpochMilli(millisUtc);
            // ClickHouse stores incorrect results when the values are out of supported range.
            DATETIME.validate(version, instant.atZone(UTC).toLocalDateTime());
            statement.setObject(index, instant.atZone(columnTimeZone.toZoneId()));
        };
    }

    private static LongWriteFunction shortDateTime64WithTimeZoneWriteFunction()
    {
        // Short timestamp(<=3) with time zone written to a DateTime64(p, 'UTC') column. Unlike the DateTime variant,
        // DateTime64 supports a far wider range, so the narrow DateTime range check is not applied.
        return (statement, index, value) -> {
            Instant instant = Instant.ofEpochMilli(unpackMillisUtc(value));
            statement.setObject(index, instant.atZone(UTC));
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            ZonedDateTime zonedDateTime = resultSet.getObject(columnIndex, ZonedDateTime.class);
            Instant instant = zonedDateTime.toInstant();
            long epochMillis = instant.toEpochMilli();
            int picosOfMilli = (int) ((long) (instant.getNano() % NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND);
            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, TimeZoneKey.getTimeZoneKey(zonedDateTime.getZone().getId()));
        });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        // High-precision timestamp with time zone (precision > 3): the value carries epoch millis + picos-of-milli.
        // The instant is written to a DateTime64(p, 'UTC') column, preserving sub-millisecond precision in UTC.
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochMillis = value.getEpochMillis();
            long picosOfMilli = value.getPicosOfMilli();
            Instant instant = Instant.ofEpochSecond(
                    Math.floorDiv(epochMillis, MILLISECONDS_PER_SECOND),
                    (Math.floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND)
                            + (picosOfMilli / PICOSECONDS_PER_NANOSECOND));
            statement.setObject(index, instant.atZone(UTC));
        });
    }

    private ColumnMapping ipAddressColumnMapping(String clickhouseType)
    {
        return ColumnMapping.sliceMapping(
                ipAddressType,
                (resultSet, columnIndex) -> {
                    // copied from IpAddressOperators.castFromVarcharToIpAddress
                    byte[] address = InetAddresses.forString(resultSet.getString(columnIndex)).getAddress();

                    byte[] bytes;
                    if (address.length == 4) {
                        bytes = new byte[16];
                        bytes[10] = (byte) 0xff;
                        bytes[11] = (byte) 0xff;
                        arraycopy(address, 0, bytes, 12, 4);
                    }
                    else if (address.length == 16) {
                        bytes = address;
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
                    }

                    return wrappedBuffer(bytes);
                },
                ipAddressWriteFunction(clickhouseType));
    }

    private static SliceWriteFunction ipAddressWriteFunction(String clickhouseType)
    {
        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return format("CAST(? AS %s)", clickhouseType);
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                try {
                    statement.setObject(index, InetAddresses.toAddrString(InetAddress.getByAddress(value.getBytes())), Types.OTHER);
                }
                catch (UnknownHostException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> javaUuidToTrinoUuid((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> statement.setObject(index, trinoUuidToJavaUuid(value), Types.OTHER);
    }

    public static boolean supportsPushdown(Variable variable, RewriteContext<ParameterizedExpression> context)
    {
        JdbcTypeHandle typeHandle = ((JdbcColumnHandle) context.getAssignment(variable.getName()))
                .getJdbcTypeHandle();
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        ClickHouseColumn column = ClickHouseColumn.of("", jdbcTypeName);
        ClickHouseDataType columnDataType = column.getDataType();
        return switch (columnDataType) {
            case FixedString, String -> true;
            default -> false;
        };
    }
}
