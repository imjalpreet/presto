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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.rule.BaseRewriter;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergHiveMetadata;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.rule.FilterPushdownUtils.getDomainPredicate;
import static com.facebook.presto.hive.rule.FilterPushdownUtils.getPredicateColumnNames;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class IcebergFilterPushdown
        implements ConnectorPlanOptimizer
{
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    protected final IcebergResourceFactory resourceFactory;
    protected final HdfsEnvironment hdfsEnvironment;
    protected final TypeManager typeManager;
    protected IcebergTransactionManager icebergTransactionManager;

    public IcebergFilterPushdown(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            IcebergTransactionManager transactionManager,
            IcebergResourceFactory resourceFactory,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager)
    {
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.icebergTransactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isPushdownFilterEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new Rewriter(
                session,
                idAllocator,
                rowExpressionService,
                functionResolution,
                functionMetadataManager,
                icebergTransactionManager,
                resourceFactory,
                hdfsEnvironment,
                typeManager), maxSubplan);
    }

    public static class Rewriter
            extends BaseRewriter
    {
        protected final IcebergResourceFactory resourceFactory;
        protected final HdfsEnvironment hdfsEnvironment;
        protected final TypeManager typeManager;
        protected IcebergTransactionManager icebergTransactionManager;

        public Rewriter(
                ConnectorSession session,
                PlanNodeIdAllocator idAllocator,
                RowExpressionService rowExpressionService,
                StandardFunctionResolution functionResolution,
                FunctionMetadataManager functionMetadataManager,
                IcebergTransactionManager icebergTransactionManager,
                IcebergResourceFactory resourceFactory,
                HdfsEnvironment hdfsEnvironment,
                TypeManager typeManager)
        {
            super(session, idAllocator, rowExpressionService, functionResolution, functionMetadataManager, tableHandle -> getConnectorMetadata(icebergTransactionManager, tableHandle));

            this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
            this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.icebergTransactionManager = requireNonNull(icebergTransactionManager, "transactionManager is null");
        }

        @Override
        protected ConnectorPushdownFilterResult getConnectorPushdownFilterResult(
                Map<String, ColumnHandle> columnHandles,
                ConnectorMetadata metadata,
                ConnectorSession session,
                RemainingExpressions remainingExpressions,
                DomainTranslator.ExtractionResult<Subfield> decomposedFilter,
                RowExpression optimizedRemainingExpression,
                Constraint<ColumnHandle> constraint,
                Optional<ConnectorTableLayoutHandle> currentLayoutHandle,
                ConnectorTableHandle tableHandle)
        {
            Table icebergTable;
            if (metadata instanceof IcebergHiveMetadata) {
                ExtendedHiveMetastore metastore = ((IcebergHiveMetadata) metadata).getMetastore();
                icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
            }
            else {
                icebergTable = getNativeIcebergTable(resourceFactory, session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
            }

            TupleDomain<ColumnHandle> unenforcedConstraint = TupleDomain.withColumnDomains(Maps.filterKeys(constraint.getSummary().getDomains().get(), not(Predicates.in(getPartitionKeyColumnHandles(icebergTable, typeManager)))));

            TupleDomain<Subfield> domainPredicate = getDomainPredicate(decomposedFilter, unenforcedConstraint);

            Set<String> predicateColumnNames = getPredicateColumnNames(optimizedRemainingExpression, domainPredicate);

            Map<String, IcebergColumnHandle> predicateColumns = predicateColumnNames.stream()
                    .map(columnHandles::get)
                    .map(IcebergColumnHandle.class::cast)
                    .collect(toImmutableMap(IcebergColumnHandle::getName, Functions.identity()));

            Optional<Set<IcebergColumnHandle>> requestedColumns = currentLayoutHandle.map(layout -> ((IcebergTableLayoutHandle) layout).getRequestedColumns()).orElse(Optional.empty());

            List<IcebergColumnHandle> partitionColumns = getPartitionKeyColumnHandles(icebergTable, typeManager);
            TupleDomain<ColumnHandle> partitionColumnPredicate = TupleDomain.withColumnDomains(Maps.filterKeys(
                            constraint.getSummary().getDomains().get(), Predicates.in(partitionColumns)));

            List<HivePartition> partitions = getPartitions(
                    tableHandle,
                    icebergTable,
                    constraint.getSummary().simplify().transform(IcebergColumnHandle.class::cast),
                    partitionColumns);

            return new ConnectorPushdownFilterResult(
                    metadata.getTableLayout(
                            session,
                            new IcebergTableLayoutHandle.Builder()
                                    .setPartitionColumns(partitionColumns)
                                    .setDomainPredicate(domainPredicate)
                                    .setRemainingPredicate(remainingExpressions.getRemainingExpression())
                                    .setPredicateColumns(predicateColumns)
                                    .setRequestedColumns(requestedColumns)
                                    .setPushdownFilterEnabled(true)
                                    .setPartitionColumnPredicate(partitionColumnPredicate)
                                    .setPartitions(Optional.ofNullable(partitions.size() == 0 ? null : partitions))
                                    .setTable((IcebergTableHandle) tableHandle)
                                    .build()),
                    remainingExpressions.getDynamicFilterExpression());
        }

        @Override
        protected boolean isPushdownFilterSupported(ConnectorSession session, TableHandle tableHandle)
        {
            return isPushdownFilterEnabled(session);
        }

        private List<HivePartition> getPartitions(
                ConnectorTableHandle tableHandle,
                Table icebergTable,
                TupleDomain<IcebergColumnHandle> predicate,
                List<IcebergColumnHandle> partitionColumns)
        {
            IcebergTableName name = IcebergTableName.from(((IcebergTableHandle) tableHandle).getTableName());
            TableScan tableScan = icebergTable.newScan()
                    .filter(toIcebergExpression(predicate))
                    .useSnapshot(getSnapshotId(icebergTable, name.getSnapshotId()).get());

            Set<HivePartition> partitions = new HashSet<>();

            try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
                for (FileScanTask fileScanTask : fileScanTasks) {
                    StructLike partition = fileScanTask.file().partition();
                    PartitionSpec spec = fileScanTask.spec();
                    Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
                    ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();

                    fieldToIndex.forEach((field, index) -> {
                        int id = field.sourceId();
                        Type type = spec.schema().findType(id);
                        Class<?> javaClass = type.typeId().javaClass();
                        Object value = partition.get(index, javaClass);

                        NullableValue partitionValue = new NullableValue(toPrestoType(type, typeManager), value);
                        Optional<IcebergColumnHandle> column = partitionColumns.stream()
                                .filter(icebergColumnHandle -> Objects.equals(icebergColumnHandle.getName(), field.name()))
                                .findAny();

                        builder.put(column.get(), partitionValue);
                    });

                    Map<ColumnHandle, NullableValue> values = builder.build();
                    HivePartition newPartition = new HivePartition(
                            ((IcebergTableHandle) tableHandle).getSchemaTableName(),
                            partition.toString(),
                            values);

                    partitions.add(newPartition);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return new ArrayList<>(partitions);
        }

        private Optional<Long> getSnapshotId(Table table, Optional<Long> snapshotId)
        {
            if (snapshotId.isPresent()) {
                return Optional.of(IcebergUtil.resolveSnapshotId(table, snapshotId.get()));
            }
            return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
        }
    }

    private static ConnectorMetadata getConnectorMetadata(IcebergTransactionManager icebergTransactionManager, TableHandle tableHandle)
    {
        requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
        ConnectorMetadata metadata = icebergTransactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");
        return metadata;
    }
}
