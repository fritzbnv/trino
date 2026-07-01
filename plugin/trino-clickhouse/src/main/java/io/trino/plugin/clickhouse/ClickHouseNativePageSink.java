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

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * A {@link ConnectorPageSink} that streams rows to ClickHouse in the native RowBinary format via the ClickHouse Java
 * Client V2, replacing the slow per-batch {@code PreparedStatement.addBatch()/executeBatch()} path of
 * {@link io.trino.plugin.jdbc.JdbcPageSink}. Rows are encoded page-by-page into a {@link PipedOutputStream} and consumed
 * by a single streaming {@code INSERT INTO <schema>.<table> FORMAT RowBinary}, so the read side (page production) and the
 * write side (HTTP upload) pipeline instead of blocking on each batch.
 * <p>
 * The per-column RowBinary layout mirrors {@link ClickHouseClient#toWriteMapping} exactly, and per top-level column
 * nullability is read back from {@code system.columns} at open time so the Nullable(T) null-flag byte matches the actual
 * created-table DDL (see {@link ClickHouseClient#getColumnDefinitionSql}). Nested element/value nullability follows the
 * connector's own DDL rule (scalars wrapped in Nullable, Array/Map left bare, Map keys never Nullable).
 */
public class ClickHouseNativePageSink
        implements ConnectorPageSink
{
    private static final int DEFAULT_HTTP_PORT = 8123;
    private static final int DEFAULT_HTTPS_PORT = 8443;
    private static final int PIPE_BUFFER_SIZE = 1 << 20;

    // ClickHouse's UTC timezone marker for BinaryStreamUtils.writeDateTime64; using UTC makes the encoder read
    // LocalDateTime.toEpochSecond(UTC), matching the LocalDateTime the JDBC write functions build in UTC.
    private static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");

    private final ConnectorPageSinkId pageSinkId;
    private final List<ColumnEncoder> columnEncoders;
    private final int dataColumnCount;
    private final boolean includePageSinkIdColumn;

    private final Client client;
    private final ExecutorService insertExecutor;
    private final PipedOutputStream pipeSink;
    private final Future<InsertResponse> insertFuture;
    private volatile boolean closed;

    public ClickHouseNativePageSink(
            ConnectorSession session,
            JdbcOutputTableHandle handle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            String connectionUrl,
            Optional<String> user,
            Optional<String> password)
    {
        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");
        requireNonNull(handle, "handle is null");
        requireNonNull(session, "session is null");
        requireNonNull(jdbcClient, "jdbcClient is null");

        List<Type> columnTypes = handle.getColumnTypes();
        this.dataColumnCount = columnTypes.size();
        this.includePageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();

        RemoteTableName remoteTableName = handle.getRemoteTableName();
        // Match BaseJdbcClient.buildInsertSql: writes go to the temporary table when present (CREATE TABLE AS SELECT and
        // fault-tolerant INSERT both stage into a temp table that finishInsert/finishCreateTable later renames), otherwise
        // to the final table. Insert AND the nullability lookup must target this same table.
        String effectiveTableName = handle.getTemporaryTableName().orElse(remoteTableName.getTableName());

        // Read the actual server-side column definitions so the Nullable(T) prefix matches the created table exactly.
        // RowBinary is positional, so match the target table's declared columns in insert-column order.
        List<String> columnNames = new ArrayList<>(handle.getColumnNames());
        List<Boolean> nullableFlags;
        try {
            nullableFlags = new ArrayList<>(readColumnNullability(session, jdbcClient, handle, remoteTableName, effectiveTableName, columnNames, columnTypes));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to read ClickHouse column metadata for native insert: " + e.getMessage(), e);
        }

        ImmutableList.Builder<ColumnEncoder> encoders = ImmutableList.builder();
        for (int i = 0; i < columnTypes.size(); i++) {
            ValueEncoder valueEncoder = valueEncoder(columnTypes.get(i));
            encoders.add(new ColumnEncoder(valueEncoder, nullableFlags.get(i)));
        }
        if (includePageSinkIdColumn) {
            // The trailing trino_page_sink_id column (BIGINT -> Nullable(Int64)) exists only under fault-tolerant execution.
            boolean pageSinkIdNullable = nullableFlags.size() > columnTypes.size() && nullableFlags.get(columnTypes.size());
            encoders.add(new ColumnEncoder(valueEncoder(BIGINT), pageSinkIdNullable));
        }
        this.columnEncoders = encoders.build();

        this.client = buildClient(connectionUrl, user, password, remoteTableName.getSchemaName());

        try {
            PipedInputStream pipeSource = new PipedInputStream(PIPE_BUFFER_SIZE);
            this.pipeSink = new PipedOutputStream(pipeSource);
            String tableName = remoteTableName.getSchemaName()
                    .map(schema -> schema + "." + effectiveTableName)
                    .orElse(effectiveTableName);
            List<String> insertColumns = includePageSinkIdColumn
                    ? ImmutableList.<String>builder().addAll(columnNames).add(handle.getPageSinkIdColumnName().orElseThrow()).build()
                    : ImmutableList.copyOf(columnNames);
            // Client V2 runs the insert synchronously by default (drains the InputStream on the calling thread), so run it
            // on a dedicated thread. appendPage writes RowBinary to the piped output stream while this thread drains the
            // piped input stream into the HTTP upload, giving true read/write pipelining. The pipe applies backpressure.
            this.insertExecutor = newSingleThreadExecutor(daemonThreadsNamed("clickhouse-native-insert-" + pageSinkId.getId()));
            this.insertFuture = insertExecutor.submit(() ->
                    client.insert(tableName, insertColumns, pipeSource, ClickHouseFormat.RowBinary).get());
        }
        catch (RuntimeException | IOException e) {
            client.close();
            throw new TrinoException(JDBC_ERROR, "Failed to start ClickHouse native insert: " + e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // If the streaming insert already failed (e.g. the server rejected the stream), surface the cause now instead of
        // writing into a broken pipe.
        if (insertFuture.isDone()) {
            throw failedInsert();
        }
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < dataColumnCount; channel++) {
                    columnEncoders.get(channel).encode(pipeSink, page.getBlock(channel), position);
                }
                if (includePageSinkIdColumn) {
                    // Non-null Int64 page sink id; a null flag is emitted first when the column is Nullable(Int64).
                    columnEncoders.get(dataColumnCount).encodeLong(pipeSink, pageSinkId.getId());
                }
            }
        }
        catch (IOException e) {
            // A broken pipe means the insert thread died; surface its cause rather than the generic IOException.
            if (insertFuture.isDone()) {
                throw failedInsert();
            }
            throw new UncheckedIOException(e);
        }
        return NOT_BLOCKED;
    }

    private TrinoException failedInsert()
    {
        try {
            insertFuture.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new TrinoException(JDBC_ERROR, "Interrupted while inserting data into ClickHouse", e);
        }
        catch (ExecutionException e) {
            return new TrinoException(JDBC_ERROR, "Failed to insert data into ClickHouse: " + rootMessage(e.getCause()), e.getCause());
        }
        // insertFuture completed normally before finish() - unexpected but not an error to raise here.
        return new TrinoException(JDBC_ERROR, "ClickHouse native insert stream closed before all pages were written");
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        closed = true;
        try (Client ignored = client) {
            // Closing the output side signals end-of-stream; the insert thread then finishes draining and the HTTP upload
            // completes. Block until the server has fully consumed the stream and acknowledged the insert.
            pipeSink.close();
            try (InsertResponse response = insertFuture.get()) {
                // response is closed by try-with-resources
                requireNonNull(response, "response is null");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(JDBC_ERROR, "Interrupted while finishing ClickHouse native insert", e);
        }
        catch (ExecutionException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to insert data into ClickHouse: " + rootMessage(e.getCause()), e.getCause());
        }
        catch (IOException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to finish ClickHouse native insert: " + e.getMessage(), e);
        }
        finally {
            insertExecutor.shutdown();
        }
        // Pass the successful page sink id, exactly like JdbcPageSink, so finishInsertTable works unchanged.
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @Override
    public void abort()
    {
        if (closed) {
            return;
        }
        closed = true;
        try (Client ignored = client) {
            // Closing the pipe breaks the insert thread's read; cancel the future and shut the executor down.
            pipeSink.close();
        }
        catch (IOException | RuntimeException e) {
            // best effort: the insert is being aborted, so a broken pipe here is expected
        }
        finally {
            insertFuture.cancel(true);
            insertExecutor.shutdownNow();
        }
    }

    private static String rootMessage(Throwable throwable)
    {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.toString() : cause.getMessage();
    }

    private static Client buildClient(String connectionUrl, Optional<String> user, Optional<String> password, Optional<String> database)
    {
        // connection-url looks like jdbc:clickhouse://host:port/db[?params]; derive the HTTP endpoint from it.
        String stripped = connectionUrl;
        if (stripped.startsWith("jdbc:")) {
            stripped = stripped.substring("jdbc:".length());
        }
        URI uri = URI.create(stripped);
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ENGLISH);
        boolean secure = scheme.equals("clickhouses") || scheme.equals("https");
        String host = uri.getHost();
        if (host == null) {
            throw new TrinoException(JDBC_ERROR, "Cannot derive ClickHouse host from connection-url: " + connectionUrl);
        }
        int port = uri.getPort();
        if (port <= 0) {
            port = secure ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT;
        }
        String endpoint = (secure ? "https://" : "http://") + host + ":" + port;

        Client.Builder builder = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user.orElse("default"))
                .setPassword(password.orElse(""))
                .compressClientRequest(true);
        database.ifPresent(builder::setDefaultDatabase);
        return builder.build();
    }

    /**
     * Returns per-column nullability (in insert-column order) so the RowBinary null-flag prefix matches the target
     * table's declared columns. A column is Nullable(T) exactly when its declared server type starts with
     * {@code Nullable(} -- the ground truth being the DDL produced by {@link ClickHouseClient#getColumnDefinitionSql},
     * which wraps a column in {@code Nullable(...)} iff {@code column.isNullable() && !isClickHouseNonNullableContainer}.
     * <p>
     * The lookup targets {@code effectiveTableName} -- the temporary table for CREATE TABLE AS SELECT / fault-tolerant
     * INSERT, else the final table (see {@code BaseJdbcClient.buildInsertSql}). When the table is not visible in
     * {@code system.columns} (e.g. a CTAS whose temp table was created on a separate connection not yet committed), this
     * falls back to the same rule the connector just used to emit the DDL: an output column (always nullable in Trino)
     * is {@code Nullable(T)} when it is a scalar and bare when it is an Array/Map/Tuple container -- i.e.
     * {@link #isNullableElement}. The same fallback covers any individual column missing from {@code system.columns}.
     */
    private static List<Boolean> readColumnNullability(
            ConnectorSession session,
            JdbcClient jdbcClient,
            JdbcOutputTableHandle handle,
            RemoteTableName remoteTableName,
            String effectiveTableName,
            List<String> columnNames,
            List<Type> columnTypes)
            throws SQLException
    {
        Map<String, Boolean> nullableByName = new HashMap<>();
        String sql = "SELECT name, type FROM system.columns WHERE database = ? AND table = ?";
        try (Connection connection = jdbcClient.getConnection(session, handle);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, remoteTableName.getSchemaName().orElse(""));
            statement.setString(2, effectiveTableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString("name");
                    String type = resultSet.getString("type");
                    nullableByName.put(name, type != null && type.startsWith("Nullable("));
                }
            }
        }

        ImmutableList.Builder<Boolean> flags = ImmutableList.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Boolean nullable = nullableByName.get(columnName);
            // Fall back to the DDL rule when the column is not (yet) in system.columns (CREATE TABLE AS SELECT).
            flags.add(nullable != null ? nullable : isNullableElement(columnTypes.get(i)));
        }
        // The trailing trino_page_sink_id column is BIGINT -> Nullable(Int64) (a scalar, so always Nullable).
        handle.getPageSinkIdColumnName().ifPresent(name ->
                flags.add(nullableByName.getOrDefault(name, Boolean.TRUE)));
        return flags.build();
    }

    // A top-level column: optionally emits the Nullable null-flag byte, then delegates to the value encoder.
    private record ColumnEncoder(ValueEncoder valueEncoder, boolean nullable)
    {
        void encode(OutputStream out, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                checkState(nullable, "Unexpected null in non-nullable ClickHouse column");
                BinaryStreamUtils.writeNull(out);
                return;
            }
            if (nullable) {
                BinaryStreamUtils.writeNonNull(out);
            }
            valueEncoder.encode(out, block, position);
        }

        void encodeLong(OutputStream out, long value)
                throws IOException
        {
            if (nullable) {
                BinaryStreamUtils.writeNonNull(out);
            }
            BinaryStreamUtils.writeInt64(out, value);
        }
    }

    // Encodes a single non-null value from a Block position into RowBinary.
    @FunctionalInterface
    private interface ValueEncoder
    {
        void encode(OutputStream out, Block block, int position)
                throws IOException;
    }

    private static ValueEncoder valueEncoder(Type type)
    {
        if (type == BOOLEAN) {
            return (out, block, position) -> BinaryStreamUtils.writeBoolean(out, BOOLEAN.getBoolean(block, position));
        }
        if (type == TINYINT) {
            return (out, block, position) -> BinaryStreamUtils.writeInt8(out, (int) TINYINT.getByte(block, position));
        }
        if (type == SMALLINT) {
            return (out, block, position) -> BinaryStreamUtils.writeInt16(out, (int) SMALLINT.getShort(block, position));
        }
        if (type == INTEGER) {
            return (out, block, position) -> BinaryStreamUtils.writeInt32(out, INTEGER.getInt(block, position));
        }
        if (type == BIGINT) {
            return (out, block, position) -> BinaryStreamUtils.writeInt64(out, BIGINT.getLong(block, position));
        }
        if (type == REAL) {
            return (out, block, position) -> BinaryStreamUtils.writeFloat32(out, intBitsToFloat(REAL.getInt(block, position)));
        }
        if (type == DOUBLE) {
            return (out, block, position) -> BinaryStreamUtils.writeFloat64(out, DOUBLE.getDouble(block, position));
        }
        if (type instanceof DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            MathContext mathContext = new MathContext(precision);
            if (decimalType.isShort()) {
                return (out, block, position) -> {
                    BigInteger unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
                    BinaryStreamUtils.writeDecimal(out, new BigDecimal(unscaledValue, scale, mathContext), precision, scale);
                };
            }
            return (out, block, position) -> {
                BigInteger unscaledValue = ((Int128) decimalType.getObject(block, position)).toBigInteger();
                BinaryStreamUtils.writeDecimal(out, new BigDecimal(unscaledValue, scale, mathContext), precision, scale);
            };
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            return (out, block, position) -> BinaryStreamUtils.writeString(out, type.getSlice(block, position).getBytes());
        }
        if (type instanceof VarbinaryType) {
            // ClickHouse String is an arbitrary byte string; write the raw bytes with a varint length prefix.
            return (out, block, position) -> BinaryStreamUtils.writeString(out, type.getSlice(block, position).getBytes());
        }
        if (type instanceof TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            if (timestampType.isShort()) {
                return (out, block, position) -> {
                    LocalDateTime dateTime = fromTrinoTimestamp(timestampType.getLong(block, position));
                    BinaryStreamUtils.writeDateTime64(out, dateTime, precision, UTC_TIMEZONE);
                };
            }
            return (out, block, position) -> {
                LocalDateTime dateTime = fromLongTrinoTimestamp((LongTimestamp) timestampType.getObject(block, position), precision);
                BinaryStreamUtils.writeDateTime64(out, dateTime, precision, UTC_TIMEZONE);
            };
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            int precision = timestampWithTimeZoneType.getPrecision();
            if (timestampWithTimeZoneType.isShort()) {
                return (out, block, position) -> {
                    Instant instant = Instant.ofEpochMilli(unpackMillisUtc(timestampWithTimeZoneType.getLong(block, position)));
                    BinaryStreamUtils.writeDateTime64(out, LocalDateTime.ofInstant(instant, UTC), precision, UTC_TIMEZONE);
                };
            }
            return (out, block, position) -> {
                LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) timestampWithTimeZoneType.getObject(block, position);
                long epochMillis = value.getEpochMillis();
                long picosOfMilli = value.getPicosOfMilli();
                Instant instant = Instant.ofEpochSecond(
                        floorDiv(epochMillis, MILLISECONDS_PER_SECOND),
                        (floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND)
                                + (picosOfMilli / PICOSECONDS_PER_NANOSECOND));
                BinaryStreamUtils.writeDateTime64(out, LocalDateTime.ofInstant(instant, UTC), precision, UTC_TIMEZONE);
            };
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            // Array elements: scalars are Nullable(T), nested Array/Map are bare (matches clickHouseElementDataType).
            ColumnEncoder elementEncoder = new ColumnEncoder(valueEncoder(elementType), isNullableElement(elementType));
            return (out, block, position) -> {
                Block array = arrayType.getObject(block, position);
                BinaryStreamUtils.writeVarInt(out, array.getPositionCount());
                for (int i = 0; i < array.getPositionCount(); i++) {
                    elementEncoder.encode(out, array, i);
                }
            };
        }
        if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            // Map keys are never Nullable; values are Nullable(T) for scalars, bare for nested containers.
            ColumnEncoder keyEncoder = new ColumnEncoder(valueEncoder(keyType), false);
            ColumnEncoder valueEncoder = new ColumnEncoder(valueEncoder(valueType), isNullableElement(valueType));
            return (out, block, position) -> {
                SqlMap sqlMap = mapType.getObject(block, position);
                int rawOffset = sqlMap.getRawOffset();
                Block keyBlock = sqlMap.getRawKeyBlock();
                Block valueBlock = sqlMap.getRawValueBlock();
                int size = sqlMap.getSize();
                BinaryStreamUtils.writeVarInt(out, size);
                for (int i = 0; i < size; i++) {
                    keyEncoder.encode(out, keyBlock, rawOffset + i);
                    valueEncoder.encode(out, valueBlock, rawOffset + i);
                }
            };
        }
        if (type instanceof RowType rowType) {
            // ClickHouse Tuple(T1, ..., Tn) RowBinary layout is simply encode(f1) encode(f2) ... encode(fn) with NO
            // count/length prefix (unlike Array/Map which carry a varint size). Each field carries its Nullable null-flag
            // byte exactly when its type is nullable-eligible (scalars: yes; nested Array/Map/Row containers: no), the
            // same rule as clickHouseElementDataType uses for the Tuple field DDL.
            List<Type> fieldTypes = rowType.getTypeParameters();
            List<ColumnEncoder> fieldEncoders = new ArrayList<>(fieldTypes.size());
            for (Type fieldType : fieldTypes) {
                fieldEncoders.add(new ColumnEncoder(valueEncoder(fieldType), isNullableElement(fieldType)));
            }
            return (out, block, position) -> {
                SqlRow sqlRow = rowType.getObject(block, position);
                int rawIndex = sqlRow.getRawIndex();
                for (int i = 0; i < fieldEncoders.size(); i++) {
                    fieldEncoders.get(i).encode(out, sqlRow.getRawFieldBlock(i), rawIndex);
                }
            };
        }
        // UUID, JSON, IPv4/IPv6 and other slice-mapped types are written to a ClickHouse String column by the JDBC path;
        // they are outside the scope of this native sink's supported set. Fail fast rather than corrupt rows.
        throw new TrinoException(JDBC_ERROR, "ClickHouse native RowBinary sink does not support column type: " + type);
    }

    private static boolean isNullableElement(Type type)
    {
        // Mirror ClickHouseClient.clickHouseElementDataType / isClickHouseNonNullableContainer: Array/Map/Tuple cannot be
        // wrapped in Nullable, so they are emitted bare; every other (scalar) element is Nullable(T).
        return !(type instanceof ArrayType) && !(type instanceof MapType) && !(type instanceof RowType);
    }
}
