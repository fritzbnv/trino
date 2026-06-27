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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * ClickHouse MERGE sink, scoped to what dbt-trino's incremental {@code merge} strategy needs: an upsert into a
 * ReplacingMergeTree target. dbt-trino's {@code merge} emits only {@code WHEN MATCHED THEN UPDATE} /
 * {@code WHEN NOT MATCHED THEN INSERT} (no delete clause); a matched row is "effectively deleted" by being overwritten,
 * which on ReplacingMergeTree means re-inserting it under the same ORDER BY key and letting the background merge collapse
 * the duplicate to the latest version. The diamond serving layer uses this to sync changed rows from the gold tables.
 * <p>
 * Under {@link io.trino.spi.connector.RowChangeParadigm#DELETE_ROW_AND_INSERT_ROW} the engine delivers, per affected
 * row, a full insert-row (for INSERT and the insert half of an UPDATE) and/or a delete-row (for DELETE and the delete
 * half of an UPDATE). This sink appends every insert-row and discards every delete-row: the insert-row of an UPDATE
 * already supersedes the prior version via ReplacingMergeTree, so no row-level deletion (an expensive ClickHouse
 * mutation) is performed.
 * <p>
 * Physical row deletion is therefore intentionally unsupported. An explicit {@code WHEN MATCHED ... THEN DELETE} (which
 * dbt-trino's merge never emits) is rejected with a clear error. Correct deduplication requires the target's ORDER BY to
 * equal the merge keys; the dbt model configures this via {@code engine='ReplacingMergeTree'} and
 * {@code order_by=<unique_key>}, and reads use {@code SELECT ... FINAL} to observe the collapsed rows.
 */
public class ClickHouseMergeSink
        implements ConnectorMergeSink
{
    private final int columnCount;
    private final ConnectorPageSinkId pageSinkId;
    private final ConnectorPageSink insertSink;

    public ClickHouseMergeSink(
            ConnectorSession session,
            JdbcMergeTableHandle mergeHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier queryModifier)
    {
        requireNonNull(session, "session is null");
        requireNonNull(mergeHandle, "mergeHandle is null");
        requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(queryModifier, "queryModifier is null");

        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");
        this.columnCount = mergeHandle.getDataColumns().size();
        this.insertSink = new JdbcPageSink(session, mergeHandle.getOutputTableHandle(), jdbcClient, pageSinkId, queryModifier, JdbcClient::buildInsertSql);
    }

    @Override
    public void storeMergedRows(Page page)
    {
        // Reject an explicit "WHEN MATCHED ... THEN DELETE": this upsert sink cannot remove rows. dbt-trino's
        // incremental merge never emits a delete clause, so this only fires for hand-written deleting MERGE statements.
        rejectExplicitDeletes(page);

        // The update half of a matched-update produces a delete-row that is part of mergePage's deletions; it is
        // intentionally ignored. ReplacingMergeTree deduplicates by ORDER BY key on background merge, so re-inserting
        // the updated row is sufficient to replace the prior version.
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);
        mergePage.getInsertionsPage().ifPresent(insertSink::appendPage);
    }

    private void rejectExplicitDeletes(Page page)
    {
        // The operation column follows the data columns (see ConnectorMergeSink page layout). A genuine MERGE delete is
        // DELETE_OPERATION_NUMBER; an update's delete half is UPDATE_DELETE_OPERATION_NUMBER and is allowed (ignored).
        Block operationBlock = page.getBlock(columnCount);
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (TINYINT.getByte(operationBlock, position) == DELETE_OPERATION_NUMBER) {
                throw new TrinoException(NOT_SUPPORTED, "ClickHouse MERGE does not support deleting rows (WHEN MATCHED ... THEN DELETE); it only supports INSERT/UPDATE upserts into a ReplacingMergeTree table");
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        insertSink.finish();
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @Override
    public void abort()
    {
        insertSink.abort();
    }
}
