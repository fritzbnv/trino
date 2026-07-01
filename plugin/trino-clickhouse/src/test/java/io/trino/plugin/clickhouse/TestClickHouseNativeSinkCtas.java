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

import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for the native RowBinary sink's CREATE TABLE AS SELECT path (previously failed with
 * "No columns found in system.columns" because the target table is not yet visible when the sink opens).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TestClickHouseNativeSinkCtas
{
    private TestingClickHouseServer clickHouseServer;
    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws Exception
    {
        clickHouseServer = new TestingClickHouseServer(TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE);
        queryRunner = ClickHouseQueryRunner.builder(clickHouseServer).build();
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        if (clickHouseServer != null) {
            clickHouseServer.close();
            clickHouseServer = null;
        }
    }

    @Test
    void testCreateTableAsSelectScalars()
    {
        String table = "ctas_scalars_" + randomNameSuffix();
        try {
            // The CTAS path: table does not exist in system.columns when the sink opens. The connector maps ClickHouse
            // String <-> Trino varbinary, so the varchar written here reads back as varbinary bytes.
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id, name, price) " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT CAST(1 AS bigint), CAST('a' AS varchar), CAST(1.5 AS double)");
            assertThat(queryRunner.execute("SELECT id, name, price FROM clickhouse.tpch." + table).getMaterializedRows())
                    .singleElement()
                    .satisfies(row -> {
                        assertThat(row.getField(0)).isEqualTo(1L);
                        assertThat((byte[]) row.getField(1)).containsExactly('a');
                        assertThat(row.getField(2)).isEqualTo(1.5);
                    });
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testCreateTableAsSelectMultiRow()
    {
        String table = "ctas_multi_" + randomNameSuffix();
        try {
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT orderkey, custkey, totalprice, orderstatus FROM tpch.tiny.orders");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo((long) queryRunner.execute("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testCreateTableAsSelectWithNulls()
    {
        String table = "ctas_nulls_" + randomNameSuffix();
        try {
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id, maybe) " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT CAST(n AS bigint), CASE WHEN n % 2 = 0 THEN CAST(NULL AS varchar) ELSE CAST('v' AS varchar) END " +
                    "FROM (VALUES 1, 2, 3, 4) t(n)");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(4L);
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table + " WHERE maybe IS NULL").getOnlyValue())
                    .isEqualTo(2L);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testInsertIntoExistingStillWorks()
    {
        String table = "insert_existing_" + randomNameSuffix();
        try {
            // varchar maps to ClickHouse String, which the connector reads back as varbinary; declare varbinary so the
            // INSERT type-checks. This test targets the sink's INSERT-into-existing path (no temporary table), not the
            // varchar mapping.
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id bigint, name varbinary) " +
                    "WITH (engine = 'MergeTree')");
            queryRunner.execute("INSERT INTO clickhouse.tpch." + table +
                    " VALUES (CAST(1 AS bigint), CAST('a' AS varbinary)), " +
                    "(CAST(2 AS bigint), CAST(NULL AS varbinary)), " +
                    "(CAST(3 AS bigint), CAST('c' AS varbinary))");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(3L);
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table + " WHERE name IS NULL").getOnlyValue())
                    .isEqualTo(1L);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testCreateTableAsSelectRowType()
    {
        String table = "ctas_row_" + randomNameSuffix();
        try {
            // Row -> ClickHouse Tuple, a non-nullable container: the CTAS fallback must emit it bare (not Nullable).
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id, r) " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT CAST(1 AS bigint), CAST(ROW(10, 'x') AS ROW(a integer, b varchar))");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(1L);
            assertThat(queryRunner.execute("SELECT r.a FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(10);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testCreateTableAsSelectDate()
    {
        String table = "ctas_date_" + randomNameSuffix();
        try {
            // DATE -> ClickHouse Date. Include a NULL to exercise the Nullable(Date) null-flag path.
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id, d) " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT CAST(n AS bigint), CASE WHEN n = 2 THEN CAST(NULL AS date) ELSE DATE '2026-07-01' END " +
                    "FROM (VALUES 1, 2) t(n)");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(2L);
            assertThat(queryRunner.execute("SELECT d FROM clickhouse.tpch." + table + " WHERE id = 1").getOnlyValue())
                    .isEqualTo(LocalDate.of(2026, 7, 1));
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table + " WHERE d IS NULL").getOnlyValue())
                    .isEqualTo(1L);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testCreateTableAsSelectUuid()
    {
        String table = "ctas_uuid_" + randomNameSuffix();
        String uuid = "12345678-1234-1234-1234-1234567890ab";
        try {
            // UUID -> ClickHouse UUID (16 bytes, ClickHouse byte order); must round-trip byte-exact.
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id, u) " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT CAST(1 AS bigint), CAST('" + uuid + "' AS uuid)");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(1L);
            assertThat(queryRunner.execute("SELECT CAST(u AS varchar) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(uuid);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }

    @Test
    void testCreateTableAsSelectNullContainers()
    {
        // A NULL whole-column value for a bare (non-Nullable) container: ClickHouse cannot wrap Array/Map/Tuple in
        // Nullable(...), so a null array/map/row column must be written as the empty/default container, not rejected.
        // Regression for "Unexpected null in non-nullable ClickHouse column" (prod superset_sit_session.sit_regions,
        // an array(varchar) that is NULL in ~98% of rows).
        String table = "ctas_nullcontainers_" + randomNameSuffix();
        try {
            queryRunner.execute("CREATE TABLE clickhouse.tpch." + table + " (id, arr, m, r) " +
                    "WITH (engine = 'MergeTree') AS " +
                    "SELECT CAST(n AS bigint), " +
                    "  CASE WHEN n = 1 THEN ARRAY['x','y'] ELSE CAST(NULL AS array(varchar)) END, " +
                    "  CASE WHEN n = 1 THEN MAP(ARRAY['k'], ARRAY[1]) ELSE CAST(NULL AS map(varchar,integer)) END, " +
                    "  CASE WHEN n = 1 THEN CAST(ROW(1,'a') AS ROW(x integer, y varchar)) ELSE CAST(NULL AS ROW(x integer, y varchar)) END " +
                    "FROM (VALUES 1, 2) t(n)");
            assertThat((long) queryRunner.execute("SELECT count(*) FROM clickhouse.tpch." + table).getOnlyValue())
                    .isEqualTo(2L);
            // The null-container rows read back as empty array/map and a default-valued row (fields null), not as errors.
            assertThat((long) queryRunner.execute("SELECT cardinality(arr) FROM clickhouse.tpch." + table + " WHERE id = 2").getOnlyValue())
                    .isEqualTo(0L);
            assertThat((long) queryRunner.execute("SELECT cardinality(map_keys(m)) FROM clickhouse.tpch." + table + " WHERE id = 2").getOnlyValue())
                    .isEqualTo(0L);
            // The non-null row still round-trips.
            assertThat(queryRunner.execute("SELECT r.x FROM clickhouse.tpch." + table + " WHERE id = 1").getOnlyValue())
                    .isEqualTo(1);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS clickhouse.tpch." + table);
        }
    }
}
