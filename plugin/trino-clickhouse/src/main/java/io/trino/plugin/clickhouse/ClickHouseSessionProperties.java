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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class ClickHouseSessionProperties
        implements SessionPropertiesProvider
{
    public static final String MAP_STRING_AS_VARCHAR = "map_string_as_varchar";
    public static final String USE_FINAL = "use_final";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ClickHouseSessionProperties(ClickHouseConfig clickHouseConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        MAP_STRING_AS_VARCHAR,
                        "Map ClickHouse String and FixedString as varchar instead of varbinary",
                        clickHouseConfig.isMapStringAsVarchar(),
                        false),
                booleanProperty(
                        USE_FINAL,
                        "Read MergeTree-family tables with the FINAL modifier so engines like ReplacingMergeTree return deduplicated rows before background merge completes",
                        clickHouseConfig.isUseFinal(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isMapStringAsVarchar(ConnectorSession session)
    {
        return session.getProperty(MAP_STRING_AS_VARCHAR, Boolean.class);
    }

    public static boolean isUseFinal(ConnectorSession session)
    {
        return session.getProperty(USE_FINAL, Boolean.class);
    }
}
