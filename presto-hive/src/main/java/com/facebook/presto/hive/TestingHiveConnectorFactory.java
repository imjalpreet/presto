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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.InternalHiveConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingHiveConnectorFactory
        implements ConnectorFactory
{
    private final Optional<ExtendedHiveMetastore> metastore;

    public TestingHiveConnectorFactory()
    {
        this.metastore = Optional.empty();
    }

    public TestingHiveConnectorFactory(ExtendedHiveMetastore metastore)
    {
        this.metastore = Optional.of(requireNonNull(metastore, "metastore is null"));
    }

    @Override
    public String getName()
    {
        return "hive";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HiveHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, metastore);
    }
}
