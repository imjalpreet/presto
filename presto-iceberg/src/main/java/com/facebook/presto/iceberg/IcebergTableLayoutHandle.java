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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final boolean pushdownFilterEnabled;
    private final TupleDomain<Subfield> domainPredicate;
    private final RowExpression remainingPredicate;
    private final Map<String, IcebergColumnHandle> predicateColumns;
    private final Optional<Set<IcebergColumnHandle>> requestedColumns;
    private final IcebergTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public IcebergTableLayoutHandle(
            @JsonProperty("domainPredicate") TupleDomain<Subfield> domainPredicate,
            @JsonProperty("remainingPredicate") RowExpression remainingPredicate,
            @JsonProperty("predicateColumns") Map<String, IcebergColumnHandle> predicateColumns,
            @JsonProperty("requestedColumns") Optional<Set<IcebergColumnHandle>> requestedColumns,
            @JsonProperty("pushdownFilterEnabled") boolean pushdownFilterEnabled,
            @JsonProperty("table") IcebergTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain)
    {
        this.domainPredicate = requireNonNull(domainPredicate, "domainPredicate is null");
        this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate is null");
        this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
        this.requestedColumns = requireNonNull(requestedColumns, "requestedColumns is null");
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
    }

    @JsonProperty
    public TupleDomain<Subfield> getDomainPredicate()
    {
        return domainPredicate;
    }

    @JsonProperty
    public RowExpression getRemainingPredicate()
    {
        return remainingPredicate;
    }

    @JsonProperty
    public Map<String, IcebergColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
    }

    @JsonProperty
    public Optional<Set<IcebergColumnHandle>> getRequestedColumns()
    {
        return requestedColumns;
    }

    @JsonProperty
    public boolean isPushdownFilterEnabled()
    {
        return pushdownFilterEnabled;
    }

    @JsonProperty
    public IcebergTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergTableLayoutHandle that = (IcebergTableLayoutHandle) o;
        return Objects.equals(domainPredicate, that.domainPredicate) &&
                Objects.equals(remainingPredicate, that.remainingPredicate) &&
                Objects.equals(predicateColumns, that.predicateColumns) &&
                Objects.equals(requestedColumns, that.requestedColumns) &&
                Objects.equals(pushdownFilterEnabled, that.pushdownFilterEnabled) &&
                Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(domainPredicate, remainingPredicate, predicateColumns, requestedColumns, pushdownFilterEnabled, table, tupleDomain);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
