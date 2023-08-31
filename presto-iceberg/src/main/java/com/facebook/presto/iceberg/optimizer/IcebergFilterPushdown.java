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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.util.FilterPushdownUtils.ConnectorPushdownFilterResult;
import com.facebook.presto.hive.util.FilterPushdownUtils.Result;
import com.facebook.presto.hive.util.FilterPushdownUtils.RowExpressionResult;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.util.FilterPushdownUtils.checkConstantBooleanExpression;
import static com.facebook.presto.hive.util.FilterPushdownUtils.extractDeterministicConjuncts;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getDomainPredicate;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getEntireColumnDomain;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getPlanNode;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getPredicateColumnNames;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getRowExpressionResult;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getSymbolToColumnMapping;
import static com.facebook.presto.hive.util.FilterPushdownUtils.getTableScanNode;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergWarningCode.ICEBERG_TABLESCAN_CONVERTED_TO_VALUESNODE;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergFilterPushdown
        implements ConnectorPlanOptimizer
{
    protected IcebergTransactionManager icebergTransactionManager;
    protected final RowExpressionService rowExpressionService;
    protected final StandardFunctionResolution functionResolution;
    protected final FunctionMetadataManager functionMetadataManager;
    protected final IcebergResourceFactory resourceFactory;

    protected final HdfsEnvironment hdfsEnvironment;

    protected final TypeManager typeManager;

    public IcebergFilterPushdown(IcebergTransactionManager transactionManager,
                                 RowExpressionService rowExpressionService,
                                 StandardFunctionResolution functionResolution,
                                 FunctionMetadataManager functionMetadataManager,
                                 IcebergResourceFactory resourceFactory,
                                 HdfsEnvironment hdfsEnvironment,
                                 TypeManager typeManager)
    {
        this.icebergTransactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
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
        return rewriteWith(new IcebergFilterPushdownRewriter(session, idAllocator), maxSubplan);
    }

    protected ConnectorPushdownFilterResult pushdownFilter(
            ConnectorSession session,
            IcebergAbstractMetadata metadata,
            ConnectorTableHandle tableHandle,
            ConnectorTransactionHandle transactionHandle,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        Result result = checkConstantBooleanExpression(rowExpressionService, functionResolution, filter, currentLayoutHandle, metadata, session);
        if (result.getConnectorPushdownFilterResult() != null) {
            return result.getConnectorPushdownFilterResult();
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        TupleDomain<ColumnHandle> entireColumnDomain = getEntireColumnDomain(result, columnHandles);

        Constraint<ColumnHandle> constraint = new Constraint<>(entireColumnDomain);

        // Extract deterministic conjuncts that apply to partition columns and specify these as Constraint#predicate
        constraint = extractDeterministicConjuncts(
                rowExpressionService,
                functionResolution,
                functionMetadataManager,
                session,
                result,
                columnHandles,
                entireColumnDomain,
                constraint);

        TupleDomain<ColumnHandle> regularColumnPredicate = TupleDomain.withColumnDomains(constraint.getSummary().getDomains().get());

        TupleDomain<Subfield> domainPredicate = getDomainPredicate(result, regularColumnPredicate);

        Set<String> predicateColumnNames = getPredicateColumnNames(result, domainPredicate);

        Map<String, IcebergColumnHandle> predicateColumns = predicateColumnNames.stream()
                .map(columnHandles::get)
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableMap(IcebergColumnHandle::getName, Functions.identity()));

        RowExpressionResult rowExpressionResult = getRowExpressionResult(rowExpressionService, functionResolution, functionMetadataManager, tableHandle, result, columnHandles);

        Optional<Set<IcebergColumnHandle>> requestedColumns = currentLayoutHandle.map(layout -> ((IcebergTableLayoutHandle) layout).getRequestedColumns()).orElse(Optional.empty());

        return new ConnectorPushdownFilterResult(
                metadata.getTableLayout(
                        session,
                        new IcebergTableLayoutHandle(
                                domainPredicate,
                                rowExpressionResult.getRemainingExpression(),
                                predicateColumns,
                                requestedColumns,
                                true,
                                (IcebergTableHandle) tableHandle)),
                rowExpressionResult.getDynamicFilterExpression());
    }

    private static String getColumnName(ConnectorSession session, IcebergAbstractMetadata metadata, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
    }

    protected IcebergAbstractMetadata getMetadata(TableHandle tableHandle)
    {
        ConnectorMetadata metadata = icebergTransactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");
        return (IcebergAbstractMetadata) metadata;
    }

    private class IcebergFilterPushdownRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        IcebergFilterPushdownRewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator)
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

            RowExpression expression = filter.getPredicate();
            TableHandle handle = tableScan.getTable();
            IcebergAbstractMetadata icebergAbstractMetadata = getMetadata(handle);

            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping = getSymbolToColumnMapping(tableScan, handle, icebergAbstractMetadata, session);

            RowExpression replacedExpression = replaceExpression(expression, symbolToColumnMapping);
            // replaceExpression() may further optimize the expression; if the resulting expression is always false, then return empty Values node
            if (FALSE_CONSTANT.equals(replacedExpression)) {
                session.getWarningCollector().add((new PrestoWarning(ICEBERG_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), IcebergFilterPushdown.class.getSimpleName()))));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }
            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(
                    session,
                    icebergAbstractMetadata,
                    handle.getConnectorHandle(),
                    handle.getTransaction(),
                    replacedExpression,
                    handle.getLayout());

            ConnectorTableLayout layout = pushdownFilterResult.getLayout();
            if (layout.getPredicate().isNone()) {
                session.getWarningCollector().add((new PrestoWarning(ICEBERG_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), IcebergFilterPushdown.class.getSimpleName()))));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }

            return getPlanNode(tableScan, handle, symbolToColumnMapping, pushdownFilterResult, layout, idAllocator);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, RewriteContext<Void> context)
        {
            TableHandle handle = tableScan.getTable();
            IcebergAbstractMetadata icebergAbstractMetadata = getMetadata(handle);
            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(
                    session,
                    icebergAbstractMetadata,
                    handle.getConnectorHandle(),
                    handle.getTransaction(),
                    TRUE_CONSTANT,
                    handle.getLayout());
            if (pushdownFilterResult.getLayout().getPredicate().isNone()) {
                session.getWarningCollector().add(new PrestoWarning(ICEBERG_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), IcebergFilterPushdown.class.getSimpleName())));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }

            return getTableScanNode(tableScan, handle, pushdownFilterResult);
        }
    }
}
