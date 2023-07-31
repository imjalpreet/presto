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
package com.facebook.presto.hive.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.HiveBucketHandle;
import com.facebook.presto.hive.HiveBucketing;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HivePartitionResult;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.FilterPushdownUtils.ConnectorPushdownFilterResult;
import com.facebook.presto.hive.util.FilterPushdownUtils.Result;
import com.facebook.presto.hive.util.FilterPushdownUtils.RowExpressionResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Functions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetPushdownFilterEnabled;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveWarningCode.HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.hive.util.FilterPushdownUtils.checkConstantBooleanExpression;
import static com.facebook.presto.hive.util.FilterPushdownUtils.extractDeterministicConjuncts;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getDomainPredicate;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getEntireColumnDomain;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getPlanNode;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getPredicateColumnNames;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getRowExpressionResult;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getSymbolToColumnMapping;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getTableScanNode;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Runs during both logical and physical phases of connector-aided plan optimization.
 * In most cases filter pushdown will occur during logical phase. However, in cases
 * when new filter is added between logical and physical phases, e.g. a filter on a join
 * key from one side of a join is added to the other side, the new filter will get
 * merged with the one already pushed down.
 */
public class HiveFilterPushdown
        implements ConnectorPlanOptimizer
{
    protected final HiveTransactionManager transactionManager;
    protected final RowExpressionService rowExpressionService;
    protected final StandardFunctionResolution functionResolution;
    protected final HivePartitionManager partitionManager;
    protected final FunctionMetadataManager functionMetadataManager;

    public HiveFilterPushdown(
            HiveTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HivePartitionManager partitionManager,
            FunctionMetadataManager functionMetadataManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(session, idAllocator), maxSubplan);
    }

    protected ConnectorPushdownFilterResult pushdownFilter(
            ConnectorSession session,
            HiveMetadata metadata,
            ConnectorTableHandle tableHandle,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        return pushdownFilter(
                session,
                metadata,
                metadata.getMetastore(),
                tableHandle,
                filter,
                currentLayoutHandle);
    }

    public ConnectorPushdownFilterResult pushdownFilter(
            ConnectorSession session,
            ConnectorMetadata metadata,
            SemiTransactionalHiveMetastore metastore,
            ConnectorTableHandle tableHandle,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        Result result = checkConstantBooleanExpression(rowExpressionService, functionResolution, filter, currentLayoutHandle, metadata, session);
        if (result.getConnectorPushdownFilterResult() != null) {
            return result.getConnectorPushdownFilterResult();
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        TupleDomain<ColumnHandle> entireColumnDomain = getEntireColumnDomain(result, columnHandles);

        if (currentLayoutHandle.isPresent()) {
            entireColumnDomain = entireColumnDomain.intersect(((HiveTableLayoutHandle) (currentLayoutHandle.get()))
                    .getPartitionColumnPredicate());
        }

        Constraint<ColumnHandle> constraint = new Constraint<>(entireColumnDomain);

        // Extract deterministic conjuncts that apply to partition columns and specify these as Constraint#predicate
        constraint = extractDeterministicConjuncts(rowExpressionService, functionResolution, functionMetadataManager, session, result, columnHandles, entireColumnDomain, constraint);

        HivePartitionResult hivePartitionResult = partitionManager.getPartitions(metastore, tableHandle, constraint, session);

        TupleDomain<Subfield> domainPredicate = getDomainPredicate(result, hivePartitionResult.getUnenforcedConstraint());

        Set<String> predicateColumnNames = getPredicateColumnNames(result, domainPredicate);

        Map<String, HiveColumnHandle> predicateColumns = predicateColumnNames.stream()
                .map(columnHandles::get)
                .map(HiveColumnHandle.class::cast)
                .collect(toImmutableMap(HiveColumnHandle::getName, Functions.identity()));

        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();

        RowExpressionResult rowExpressionResult = getRowExpressionResult(rowExpressionService, functionResolution, functionMetadataManager, tableHandle, result, columnHandles);

        MetastoreContext context = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), metastore.getColumnConverterProvider());
        Table table = metastore.getTable(context, hiveTableHandle)
                .orElseThrow(() -> new TableNotFoundException(tableName));
        String layoutString = createTableLayoutString(
                session,
                rowExpressionService,
                tableName,
                hivePartitionResult.getBucketHandle(),
                hivePartitionResult.getBucketFilter(),
                rowExpressionResult.getRemainingExpression(),
                domainPredicate);

        Optional<Set<HiveColumnHandle>> requestedColumns = currentLayoutHandle.map(layout -> ((HiveTableLayoutHandle) layout).getRequestedColumns()).orElse(Optional.empty());

        boolean appendRowNumbereEnabled = currentLayoutHandle.map(layout -> ((HiveTableLayoutHandle) layout).isAppendRowNumberEnabled()).orElse(false);
        return new ConnectorPushdownFilterResult(
                metadata.getTableLayout(
                        session,
                        new HiveTableLayoutHandle.Builder()
                                .setSchemaTableName(tableName)
                                .setTablePath(table.getStorage().getLocation())
                                .setPartitionColumns(hivePartitionResult.getPartitionColumns())
                                .setDataColumns(pruneColumnComments(hivePartitionResult.getDataColumns()))
                                .setTableParameters(hivePartitionResult.getTableParameters())
                                .setDomainPredicate(domainPredicate)
                                .setRemainingPredicate(rowExpressionResult.getRemainingExpression())
                                .setPredicateColumns(predicateColumns)
                                .setPartitionColumnPredicate(hivePartitionResult.getEnforcedConstraint())
                                .setPartitions(hivePartitionResult.getPartitions())
                                .setBucketHandle(hivePartitionResult.getBucketHandle())
                                .setBucketFilter(hivePartitionResult.getBucketFilter())
                                .setPushdownFilterEnabled(true)
                                .setLayoutString(layoutString)
                                .setRequestedColumns(requestedColumns)
                                .setPartialAggregationsPushedDown(false)
                                .setAppendRowNumberEnabled(appendRowNumbereEnabled)
                                .setHiveTableHandle(hiveTableHandle)
                                .build()),
                rowExpressionResult.getDynamicFilterExpression());
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        Rewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitFilter(FilterNode filter, RewriteContext<Void> context)
        {
            if (!(filter.getSource() instanceof TableScanNode)) {
                return visitPlan(filter, context);
            }

            TableScanNode tableScan = (TableScanNode) filter.getSource();
            if (!isPushdownFilterSupported(session, tableScan.getTable())) {
                return filter;
            }

            RowExpression expression = filter.getPredicate();
            TableHandle handle = tableScan.getTable();
            HiveMetadata hiveMetadata = getMetadata(handle);

            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping = getSymbolToColumnMapping(tableScan, handle, hiveMetadata, session);

            RowExpression replacedExpression = replaceExpression(expression, symbolToColumnMapping);
            // replaceExpression() may further optimize the expression; if the resulting expression is always false, then return empty Values node
            if (FALSE_CONSTANT.equals(replacedExpression)) {
                session.getWarningCollector().add((new PrestoWarning(HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), HiveFilterPushdown.class.getSimpleName()))));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }
            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(
                    session,
                    hiveMetadata,
                    handle.getConnectorHandle(),
                    replacedExpression,
                    handle.getLayout());

            ConnectorTableLayout layout = pushdownFilterResult.getLayout();
            if (layout.getPredicate().isNone()) {
                session.getWarningCollector().add((new PrestoWarning(HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), HiveFilterPushdown.class.getSimpleName()))));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }

            return getPlanNode(tableScan, handle, symbolToColumnMapping, pushdownFilterResult, layout, idAllocator);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, RewriteContext<Void> context)
        {
            if (!isPushdownFilterSupported(session, tableScan.getTable())) {
                return tableScan;
            }
            TableHandle handle = tableScan.getTable();
            HiveMetadata hiveMetadata = getMetadata(handle);
            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(
                    session,
                    hiveMetadata,
                    handle.getConnectorHandle(),
                    TRUE_CONSTANT,
                    handle.getLayout());
            if (pushdownFilterResult.getLayout().getPredicate().isNone()) {
                session.getWarningCollector().add(new PrestoWarning(HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), HiveFilterPushdown.class.getSimpleName())));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }

            return getTableScanNode(tableScan, handle, pushdownFilterResult);
        }
    }

    protected HiveMetadata getMetadata(TableHandle tableHandle)
    {
        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof HiveMetadata, "metadata must be HiveMetadata");
        return (HiveMetadata) metadata;
    }

    protected boolean isPushdownFilterSupported(ConnectorSession session, TableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorHandle() instanceof HiveTableHandle, "pushdownFilter is never supported on a non-hive TableHandle");
        if (((HiveTableHandle) tableHandle.getConnectorHandle()).getAnalyzePartitionValues().isPresent()) {
            return false;
        }

        boolean pushdownFilterEnabled = HiveSessionProperties.isPushdownFilterEnabled(session);
        if (pushdownFilterEnabled) {
            HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(getMetadata(tableHandle).getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
            if (hiveStorageFormat == HiveStorageFormat.ORC || hiveStorageFormat == HiveStorageFormat.DWRF || hiveStorageFormat == HiveStorageFormat.PARQUET && isParquetPushdownFilterEnabled(session)) {
                return true;
            }
        }
        return false;
    }

    private static List<Column> pruneColumnComments(List<Column> columns)
    {
        return columns.stream()
                .map(column -> new Column(column.getName(), column.getType(), Optional.empty(), column.getTypeMetadata()))
                .collect(toImmutableList());
    }

    private static String createTableLayoutString(
            ConnectorSession session,
            RowExpressionService rowExpressionService,
            SchemaTableName tableName,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketing.HiveBucketFilter> bucketFilter,
            RowExpression remainingPredicate,
            TupleDomain<Subfield> domainPredicate)
    {
        return toStringHelper(tableName.toString())
                .omitNullValues()
                .add("buckets", bucketHandle.map(HiveBucketHandle::getReadBucketCount).orElse(null))
                .add("bucketsToKeep", bucketFilter.map(HiveBucketing.HiveBucketFilter::getBucketsToKeep).orElse(null))
                .add("filter", TRUE_CONSTANT.equals(remainingPredicate) ? null : rowExpressionService.formatRowExpression(session, remainingPredicate))
                .add("domains", domainPredicate.isAll() ? null : domainPredicate.toString(session.getSqlFunctionProperties()))
                .toString();
    }
}
