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
package com.facebook.presto.hive.util;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.hive.BaseHiveTableLayoutHandle;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.expressions.DynamicFilters.isDynamicFilter;
import static com.facebook.presto.expressions.DynamicFilters.removeNestedDynamicFilters;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class FilterPushdownUtils
{
    private FilterPushdownUtils() {}

    private static final ConnectorTableLayout EMPTY_TABLE_LAYOUT = new ConnectorTableLayout(
            new ConnectorTableLayoutHandle() {},
            Optional.empty(),
            TupleDomain.none(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            emptyList());

    public static Result checkConstantBooleanExpression(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle,
            ConnectorMetadata metadata,
            ConnectorSession session)
    {
        checkArgument(!FALSE_CONSTANT.equals(filter), "Cannot pushdown filter that is always false");

        ConnectorPushdownFilterResult connectorPushdownFilterResult;
        if (TRUE_CONSTANT.equals(filter) && currentLayoutHandle.isPresent()) {
            connectorPushdownFilterResult = new ConnectorPushdownFilterResult(metadata.getTableLayout(session, currentLayoutHandle.get()), TRUE_CONSTANT);
            return new Result(connectorPushdownFilterResult);
        }

        DomainTranslator.ExtractionResult<Subfield> decomposedFilter = getDecomposedFilter(rowExpressionService, functionResolution, session, filter, currentLayoutHandle);

        if (decomposedFilter.getTupleDomain().isNone()) {
            connectorPushdownFilterResult = new ConnectorPushdownFilterResult(EMPTY_TABLE_LAYOUT, FALSE_CONSTANT);
            return new Result(connectorPushdownFilterResult);
        }

        RowExpression optimizedRemainingExpression = rowExpressionService.getExpressionOptimizer().optimize(decomposedFilter.getRemainingExpression(), OPTIMIZED, session);
        if (optimizedRemainingExpression instanceof ConstantExpression) {
            ConstantExpression constantExpression = (ConstantExpression) optimizedRemainingExpression;
            if (FALSE_CONSTANT.equals(constantExpression) || constantExpression.getValue() == null) {
                connectorPushdownFilterResult = new ConnectorPushdownFilterResult(EMPTY_TABLE_LAYOUT, FALSE_CONSTANT);
                return new Result(connectorPushdownFilterResult);
            }
        }

        return new Result(decomposedFilter, optimizedRemainingExpression);
    }

    public static TupleDomain<ColumnHandle> getEntireColumnDomain(
            Result result,
            Map<String, ColumnHandle> columnHandles,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        TupleDomain<ColumnHandle> entireColumnDomain = result.getDecomposedFilter().getTupleDomain()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(columnHandles::get);

        if (currentLayoutHandle.isPresent()) {
            entireColumnDomain = entireColumnDomain.intersect(((BaseHiveTableLayoutHandle) (currentLayoutHandle.get()))
                    .getPartitionColumnPredicate());
        }

        return entireColumnDomain;
    }

    public static Constraint<ColumnHandle> extractDeterministicConjuncts(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            ConnectorSession session,
            Result result,
            Map<String, ColumnHandle> columnHandles,
            TupleDomain<ColumnHandle> entireColumnDomain,
            Constraint<ColumnHandle> constraint)
    {
        if (!TRUE_CONSTANT.equals(result.getDecomposedFilter().getRemainingExpression())) {
            LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                    rowExpressionService.getDeterminismEvaluator(), functionResolution, functionMetadataManager);
            RowExpression deterministicPredicate = logicalRowExpressions.filterDeterministicConjuncts(
                    result.getDecomposedFilter().getRemainingExpression());
            if (!TRUE_CONSTANT.equals(deterministicPredicate)) {
                ConstraintEvaluator evaluator = new ConstraintEvaluator(
                        rowExpressionService, session, columnHandles, deterministicPredicate);
                constraint = new Constraint<>(entireColumnDomain, evaluator::isCandidate);
            }
        }
        return constraint;
    }

    public static TupleDomain<Subfield> getDomainPredicate(Result result, TupleDomain<ColumnHandle> unenforcedConstraint)
    {
        return withColumnDomains(ImmutableMap.<Subfield, Domain>builder()
                .putAll(unenforcedConstraint
                        .transform(FilterPushdownUtils::toSubfield)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .putAll(result.getDecomposedFilter().getTupleDomain()
                        .transform(subfield -> !isEntireColumn(subfield) ? subfield : null)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .build());
    }

    public static Set<String> getPredicateColumnNames(Result result, TupleDomain<Subfield> domainPredicate)
    {
        Set<String> predicateColumnNames = new HashSet<>();
        domainPredicate.getDomains().get().keySet().stream()
                .map(Subfield::getRootName)
                .forEach(predicateColumnNames::add);
        // Include only columns referenced in the optimized expression. Although the expression is sent to the worker node
        // unoptimized, the worker is expected to optimize the expression before executing.
        extractVariableExpressions(result.getOptimizedRemainingExpression()).stream()
                .map(VariableReferenceExpression::getName)
                .forEach(predicateColumnNames::add);

        return predicateColumnNames;
    }

    public static RowExpressionResult getRowExpressionResult(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            ConnectorTableHandle tableHandle,
            Result result,
            Map<String, ColumnHandle> columnHandles)
    {
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                rowExpressionService.getDeterminismEvaluator(), functionResolution, functionMetadataManager);
        List<RowExpression> conjuncts = extractConjuncts(result.getDecomposedFilter().getRemainingExpression());
        ImmutableList.Builder<RowExpression> dynamicConjuncts = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : conjuncts) {
            if (isDynamicFilter(conjunct) || useDynamicFilter(conjunct, tableHandle, columnHandles)) {
                dynamicConjuncts.add(conjunct);
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }
        RowExpression dynamicFilterExpression = logicalRowExpressions.combineConjuncts(dynamicConjuncts.build());
        RowExpression remainingExpression = logicalRowExpressions.combineConjuncts(staticConjuncts.build());
        remainingExpression = removeNestedDynamicFilters(remainingExpression);
        return new RowExpressionResult(dynamicFilterExpression, remainingExpression);
    }

    public static BiMap<VariableReferenceExpression, VariableReferenceExpression> getSymbolToColumnMapping(
            TableScanNode tableScan,
            TableHandle handle,
            ConnectorMetadata metadata,
            ConnectorSession session)
    {
        BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping =
                tableScan.getAssignments().entrySet().stream().collect(toImmutableBiMap(
                        Map.Entry::getKey,
                        entry -> new VariableReferenceExpression(
                                Optional.empty(),
                                getColumnName(session, metadata, handle.getConnectorHandle(), entry.getValue()),
                                entry.getKey().getType())));
        return symbolToColumnMapping;
    }

    private static String getColumnName(
            ConnectorSession session,
            ConnectorMetadata metadata,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
    }

    /**
     * If this method returns true, the expression will not be pushed inside the TableScan node.
     * This is exposed for dynamic or computed column functionality support.
     * Consider the following case:
     * Read a base row from the file. Modify the row to add a new column or update an existing column
     * all inside the TableScan. If filters are pushed down inside the TableScan, it would try to apply
     * it on base row. In these cases, override this method and return true. This will prevent the
     * expression from being pushed into the TableScan but will wrap the TableScanNode in a FilterNode.
     * @param expression expression to be evaluated.
     * @param tableHandle tableHandler where the expression to be evaluated.
     * @param columnHandleMap column name to column handle Map for all columns in the table.
     * @return true, if this expression should not be pushed inside the table scan, else false.
     */
    public static boolean useDynamicFilter(RowExpression expression, ConnectorTableHandle tableHandle, Map<String, ColumnHandle> columnHandleMap)
    {
        return false;
    }

    public static TableScanNode getTableScanNode(TableScanNode tableScan, TableHandle handle, ConnectorPushdownFilterResult pushdownFilterResult)
    {
        TableScanNode node = new TableScanNode(
                tableScan.getSourceLocation(),
                tableScan.getId(),
                new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), Optional.of(pushdownFilterResult.getLayout().getHandle())),
                tableScan.getOutputVariables(),
                tableScan.getAssignments(),
                tableScan.getTableConstraints(),
                pushdownFilterResult.getLayout().getPredicate(),
                TupleDomain.all());

        RowExpression unenforcedFilter = pushdownFilterResult.getUnenforcedConstraint();
        if (!TRUE_CONSTANT.equals(unenforcedFilter)) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unenforced filter found %s but not handled", unenforcedFilter));
        }
        return node;
    }

    public static PlanNode getPlanNode(
            TableScanNode tableScan,
            TableHandle handle,
            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping,
            ConnectorPushdownFilterResult pushdownFilterResult,
            ConnectorTableLayout layout,
            PlanNodeIdAllocator idAllocator)
    {
        TableScanNode node = new TableScanNode(
                tableScan.getSourceLocation(),
                tableScan.getId(),
                new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), Optional.of(pushdownFilterResult.getLayout().getHandle())),
                tableScan.getOutputVariables(),
                tableScan.getAssignments(),
                tableScan.getTableConstraints(),
                layout.getPredicate(),
                TupleDomain.all());

        RowExpression unenforcedFilter = pushdownFilterResult.getUnenforcedConstraint();
        if (!TRUE_CONSTANT.equals(unenforcedFilter)) {
            return new FilterNode(tableScan.getSourceLocation(), idAllocator.getNextId(), node, replaceExpression(unenforcedFilter, symbolToColumnMapping.inverse()));
        }

        return node;
    }

    private static DomainTranslator.ExtractionResult<Subfield> getDecomposedFilter(
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            ConnectorSession session,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        // Split the filter into 3 groups of conjuncts:
        //  - range filters that apply to entire columns,
        //  - range filters that apply to subfields,
        //  - the rest. Intersect these with possibly pre-existing filters.
        DomainTranslator.ExtractionResult<Subfield> decomposedFilter = rowExpressionService.getDomainTranslator()
                .fromPredicate(session, filter, new SubfieldExtractor(
                        functionResolution, rowExpressionService.getExpressionOptimizer(), session).toColumnExtractor());
        if (currentLayoutHandle.isPresent() && currentLayoutHandle.get() instanceof BaseHiveTableLayoutHandle) {
            BaseHiveTableLayoutHandle currentHiveLayout = (BaseHiveTableLayoutHandle) currentLayoutHandle.get();
            decomposedFilter = intersectExtractionResult(new DomainTranslator.ExtractionResult(
                    currentHiveLayout.getDomainPredicate(), currentHiveLayout.getRemainingPredicate()), decomposedFilter);
        }
        return decomposedFilter;
    }

    private static DomainTranslator.ExtractionResult intersectExtractionResult(
            DomainTranslator.ExtractionResult left,
            DomainTranslator.ExtractionResult right)
    {
        RowExpression newRemainingExpression;
        if (right.getRemainingExpression().equals(TRUE_CONSTANT)) {
            newRemainingExpression = left.getRemainingExpression();
        }
        else if (left.getRemainingExpression().equals(TRUE_CONSTANT)) {
            newRemainingExpression = right.getRemainingExpression();
        }
        else {
            newRemainingExpression = and(left.getRemainingExpression(), right.getRemainingExpression());
        }
        return new DomainTranslator.ExtractionResult(
                left.getTupleDomain().intersect(right.getTupleDomain()), newRemainingExpression);
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static Subfield toSubfield(ColumnHandle columnHandle)
    {
        return new Subfield(((HiveColumnHandle) columnHandle).getName(), ImmutableList.of());
    }

    public static class Result
    {
        public final DomainTranslator.ExtractionResult<Subfield> decomposedFilter;
        public final RowExpression optimizedRemainingExpression;
        public final ConnectorPushdownFilterResult connectorPushdownFilterResult;

        public Result(ConnectorPushdownFilterResult connectorPushdownFilterResult)
        {
            this(null, null, connectorPushdownFilterResult);
        }

        public Result(DomainTranslator.ExtractionResult<Subfield> decomposedFilter, RowExpression optimizedRemainingExpression)
        {
            this(decomposedFilter, optimizedRemainingExpression, null);
        }

        public Result(
                DomainTranslator.ExtractionResult<Subfield> decomposedFilter,
                RowExpression optimizedRemainingExpression,
                ConnectorPushdownFilterResult connectorPushdownFilterResult)
        {
            this.decomposedFilter = decomposedFilter;
            this.optimizedRemainingExpression = optimizedRemainingExpression;
            this.connectorPushdownFilterResult = connectorPushdownFilterResult;
        }

        public DomainTranslator.ExtractionResult<Subfield> getDecomposedFilter()
        {
            return decomposedFilter;
        }

        public RowExpression getOptimizedRemainingExpression()
        {
            return optimizedRemainingExpression;
        }

        public ConnectorPushdownFilterResult getConnectorPushdownFilterResult()
        {
            return connectorPushdownFilterResult;
        }
    }

    public static class ConnectorPushdownFilterResult
    {
        private final ConnectorTableLayout layout;
        private final RowExpression unenforcedConstraint;

        public ConnectorPushdownFilterResult(ConnectorTableLayout layout, RowExpression unenforcedConstraint)
        {
            this.layout = requireNonNull(layout, "layout is null");
            this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
        }

        public ConnectorTableLayout getLayout()
        {
            return layout;
        }

        public RowExpression getUnenforcedConstraint()
        {
            return unenforcedConstraint;
        }
    }

    private static class ConstraintEvaluator
    {
        private final Map<String, ColumnHandle> assignments;
        private final RowExpressionService evaluator;
        private final ConnectorSession session;
        private final RowExpression expression;
        private final Set<ColumnHandle> arguments;

        public ConstraintEvaluator(
                RowExpressionService evaluator,
                ConnectorSession session,
                Map<String, ColumnHandle> assignments,
                RowExpression expression)
        {
            this.assignments = assignments;
            this.evaluator = evaluator;
            this.session = session;
            this.expression = expression;

            arguments = ImmutableSet.copyOf(extractVariableExpressions(expression)).stream()
                    .map(VariableReferenceExpression::getName)
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }

            Function<VariableReferenceExpression, Object> variableResolver = variable -> {
                ColumnHandle column = assignments.get(variable.getName());
                checkArgument(column != null, "Missing column assignment for %s", variable);

                if (!bindings.containsKey(column)) {
                    return variable;
                }

                return bindings.get(column).getValue();
            };

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = null;
            try {
                optimized = evaluator.getExpressionOptimizer().optimize(expression, OPTIMIZED, session, variableResolver);
            }
            catch (PrestoException e) {
                propagateIfUnhandled(e);
                return true;
            }

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized) && optimized != null
                    && (!(optimized instanceof ConstantExpression) || !((ConstantExpression) optimized).isNull());
        }

        private static void propagateIfUnhandled(PrestoException e)
                throws PrestoException
        {
            int errorCode = e.getErrorCode().getCode();
            if (errorCode == DIVISION_BY_ZERO.toErrorCode().getCode()
                    || errorCode == INVALID_CAST_ARGUMENT.toErrorCode().getCode()
                    || errorCode == INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode()
                    || errorCode == NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().getCode()) {
                return;
            }

            throw e;
        }
    }

    public static Set<VariableReferenceExpression> extractVariableExpressions(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    private static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    public static class RowExpressionResult
    {
        public final RowExpression dynamicFilterExpression;
        public final RowExpression remainingExpression;

        public RowExpressionResult(RowExpression dynamicFilterExpression, RowExpression remainingExpression)
        {
            this.dynamicFilterExpression = dynamicFilterExpression;
            this.remainingExpression = remainingExpression;
        }

        public RowExpression getDynamicFilterExpression()
        {
            return dynamicFilterExpression;
        }

        public RowExpression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
