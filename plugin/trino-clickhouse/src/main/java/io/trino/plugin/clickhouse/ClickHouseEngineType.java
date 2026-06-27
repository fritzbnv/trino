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

public enum ClickHouseEngineType
{
    STRIPELOG("StripeLog"),
    LOG("Log"),
    TINYLOG("TinyLog"),
    MERGETREE("MergeTree()"),
    // ReplacingMergeTree collapses rows that share the same ORDER BY key on background merge, keeping the last
    // inserted version. It is the engine the "diamond" serving layer uses so dbt incremental MERGE can be
    // implemented as plain INSERTs of changed rows (see ClickHouseMergeSink) instead of row-level mutations.
    REPLACINGMERGETREE("ReplacingMergeTree()");

    private final String engineType;

    ClickHouseEngineType(String engineType)
    {
        this.engineType = engineType;
    }

    public String getEngineType()
    {
        return this.engineType;
    }

    /**
     * Whether this engine belongs to the MergeTree family, which requires an ORDER BY clause (the connector
     * defaults to {@code ORDER BY tuple()}) and supports the indexes used for row-level DELETE and MERGE.
     */
    public boolean isMergeTreeFamily()
    {
        return this == MERGETREE || this == REPLACINGMERGETREE;
    }
}
