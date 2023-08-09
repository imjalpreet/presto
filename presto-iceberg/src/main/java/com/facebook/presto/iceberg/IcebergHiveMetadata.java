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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.hive.HiveMetadata.TABLE_COMMENT;
import static com.facebook.presto.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getWorkerType;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFileFormat;
import static com.facebook.presto.iceberg.IcebergTableProperties.getPartitioning;
import static com.facebook.presto.iceberg.IcebergTableProperties.getTableLocation;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.iceberg.IcebergUtil.getTableComment;
import static com.facebook.presto.iceberg.IcebergUtil.isIcebergTable;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.TableType.DATA;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.WorkerType.JAVA;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class IcebergHiveMetadata
        extends IcebergAbstractMetadata
{
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;

    public IcebergHiveMetadata(
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        super(typeManager, commitTaskCodec);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        return metastore.getAllDatabases(metastoreContext);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        if (getWorkerType(session).equals(JAVA)) {
            return super.getTableLayout(session, handle);
        }

        IcebergTableLayoutHandle icebergTableLayoutHandle = (IcebergTableLayoutHandle) handle;

        IcebergTableHandle tableHandle = icebergTableLayoutHandle.getTable();
        IcebergTableName name = IcebergTableName.from((tableHandle).getTableName());
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        Optional<Table> hiveTable = metastore.getTable(metastoreContext, tableHandle.getSchemaName(), name.getTableName());
        if (!hiveTable.isPresent()) {
            return null;
        }
        if (!isIcebergTable(hiveTable.get())) {
            throw new UnknownTableTypeException(tableHandle.getSchemaTableName());
        }

        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, tableHandle.getSchemaTableName());

        TupleDomain<ColumnHandle> predicate;
        RowExpression subfieldPredicate;
        if (icebergTableLayoutHandle.isPushdownFilterEnabled()) {
            predicate = icebergTableLayoutHandle.getDomainPredicate()
                    .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                    .transform(icebergTableLayoutHandle.getPredicateColumns()::get)
                    .transform(ColumnHandle.class::cast);

            // capture subfields from domainPredicate to add to remainingPredicate
            // so those filters don't get lost
            Map<String, Type> columnTypes = getColumns(icebergTable.schema(), icebergTable.spec(), typeManager).stream()
                    .collect(toImmutableMap(IcebergColumnHandle::getName, icebergColumnHandle -> getColumnMetadata(session, tableHandle, icebergColumnHandle).getType()));
            SubfieldExtractor subfieldExtractor = new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session);

            subfieldPredicate = rowExpressionService.getDomainTranslator().toPredicate(
                    icebergTableLayoutHandle.getDomainPredicate()
                            .transform(subfield -> !isEntireColumn(subfield) ? subfield : null)
                            .transform(subfield -> subfieldExtractor.toRowExpression(subfield, columnTypes.get(subfield.getRootName()))));
        }
        else {
            // TODO: Need to implement, still incomplete
            predicate = TupleDomain.none();
            subfieldPredicate = TRUE_CONSTANT;
        }

        // combine subfieldPredicate with remainingPredicate
        List<RowExpression> predicatesToCombine = ImmutableList.of(subfieldPredicate, icebergTableLayoutHandle.getRemainingPredicate()).stream()
                .filter(p -> !p.equals(TRUE_CONSTANT))
                .collect(toImmutableList());
        RowExpression combinedRemainingPredicate = binaryExpression(SpecialFormExpression.Form.AND, predicatesToCombine);

        return new ConnectorTableLayout(
                icebergTableLayoutHandle,
                Optional.empty(),
                predicate,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.of(combinedRemainingPredicate));
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        verify(name.getTableType() == DATA, "Wrong table type: " + name.getTableType());

        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        Optional<Table> hiveTable = metastore.getTable(metastoreContext, tableName.getSchemaName(), name.getTableName());
        if (!hiveTable.isPresent()) {
            return null;
        }
        if (!isIcebergTable(hiveTable.get())) {
            throw new UnknownTableTypeException(tableName);
        }

        org.apache.iceberg.Table table = getHiveIcebergTable(metastore, hdfsEnvironment, session, tableName);
        Optional<Long> snapshotId = getSnapshotId(table, name.getSnapshotId());

        return new IcebergTableHandle(
                tableName.getSchemaName(),
                name.getTableName(),
                name.getTableType(),
                snapshotId,
                TupleDomain.all());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (getWorkerType(session).equals(JAVA)) {
            return super.getTableLayouts(session, table, constraint, desiredColumns);
        }

        IcebergTableHandle handle = (IcebergTableHandle) table;

        IcebergTableName name = IcebergTableName.from(((IcebergTableHandle) table).getTableName());
        verify(name.getTableType() == DATA, "Wrong table type: " + name.getTableType());

        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        Optional<Table> hiveTable = metastore.getTable(metastoreContext, ((IcebergTableHandle) table).getSchemaName(), name.getTableName());
        if (!hiveTable.isPresent()) {
            return null;
        }
        if (!isIcebergTable(hiveTable.get())) {
            throw new UnknownTableTypeException(((IcebergTableHandle) table).getSchemaTableName());
        }

        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, ((IcebergTableHandle) table).getSchemaTableName());

        Map<String, IcebergColumnHandle> predicateColumns = constraint.getSummary().getDomains().get().keySet().stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableMap(IcebergColumnHandle::getName, Functions.identity()));

        TupleDomain<ColumnHandle> partitionColumnPredicate = TupleDomain.withColumnDomains(Maps.filterKeys(constraint.getSummary().getDomains().get(), Predicates.in(getPartitionKeyColumnHandles(icebergTable, typeManager))));
        Optional<Set<IcebergColumnHandle>> requestedColumns = desiredColumns.map(columns -> columns.stream().map(column -> (IcebergColumnHandle) column).collect(toImmutableSet()));
        ConnectorTableLayout layout = getTableLayout(session, new IcebergTableLayoutHandle(getPartitionKeyColumnHandles(icebergTable, typeManager), constraint.getSummary().transform(IcebergAbstractMetadata::toSubfield), TRUE_CONSTANT, predicateColumns, partitionColumnPredicate, requestedColumns, true, handle, TupleDomain.all()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName);
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() == DATA) {
            return Optional.empty();
        }

        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        Optional<Table> hiveTable = metastore.getTable(metastoreContext, tableName.getSchemaName(), name.getTableName());
        if (!hiveTable.isPresent() || !isIcebergTable(hiveTable.get())) {
            return Optional.empty();
        }

        org.apache.iceberg.Table table = getHiveIcebergTable(metastore, hdfsEnvironment, session, new SchemaTableName(tableName.getSchemaName(), name.getTableName()));

        return getIcebergSystemTable(tableName, table);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        return metastore
                .getAllTables(metastoreContext, schemaName.get())
                .orElseGet(() -> metastore.getAllDatabases(metastoreContext))
                .stream()
                .map(table -> new SchemaTableName(schemaName.get(), table))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, table.getSchemaTableName());
        return getColumns(icebergTable.schema(), icebergTable.spec(), typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = getSchemaLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(USER)
                .setOwnerName(session.getUser())
                .build();

        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        metastore.createDatabase(metastoreContext, database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(schemaName)).isEmpty() ||
                !listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        metastore.dropDatabase(metastoreContext, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        metastore.renameDatabase(metastoreContext, source, target);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        Database database = metastore.getDatabase(metastoreContext, schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        String targetPath = getTableLocation(tableMetadata.getProperties());
        if (targetPath == null) {
            Optional<String> location = database.getLocation();
            if (!location.isPresent() || location.get().isEmpty()) {
                throw new PrestoException(NOT_SUPPORTED, "Database " + schemaName + " location is not set");
            }

            Path databasePath = new Path(location.get());
            Path resultPath = new Path(databasePath, tableName);
            targetPath = resultPath.toString();
        }

        TableOperations operations = new HiveTableOperations(
                metastore,
                new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER),
                hdfsEnvironment,
                hdfsContext,
                schemaName,
                tableName,
                session.getUser(),
                targetPath);
        if (operations.current() != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builderWithExpectedSize(2);
        FileFormat fileFormat = getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        TableMetadata metadata = newTableMetadata(schema, partitionSpec, targetPath, propertiesBuilder.build());

        transaction = createTableTransaction(tableName, operations, metadata);

        return new IcebergWritableTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(metadata.schema()),
                PartitionSpecParser.toJson(metadata.spec()),
                getColumns(metadata.schema(), metadata.spec(), typeManager),
                targetPath,
                fileFormat,
                metadata.properties());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, table.getSchemaTableName());

        return beginIcebergTableInsert(table, icebergTable);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        // TODO: support path override in Iceberg table creation
        org.apache.iceberg.Table table = getHiveIcebergTable(metastore, hdfsEnvironment, session, handle.getSchemaTableName());
        if (table.properties().containsKey(OBJECT_STORE_PATH) ||
                table.properties().containsKey("write.folder-storage.path") || // Removed from Iceberg as of 0.14.0, but preserved for backward compatibility
                table.properties().containsKey(WRITE_METADATA_LOCATION) ||
                table.properties().containsKey(WRITE_DATA_LOCATION)) {
            throw new PrestoException(NOT_SUPPORTED, "Table " + handle.getSchemaTableName() + " contains Iceberg path override properties and cannot be dropped from Presto");
        }
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        metastore.dropTable(metastoreContext, handle.getSchemaName(), handle.getTableName(), true);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        metastore.renameTable(metastoreContext, handle.getSchemaName(), handle.getTableName(), newTable.getSchemaName(), newTable.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, handle.getSchemaTableName());
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType()), column.getComment()).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, icebergTableHandle.getSchemaTableName());
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, icebergTableHandle.getSchemaTableName());
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    @Override
    protected ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
    {
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
        if (!metastore.getTable(metastoreContext, table.getSchemaName(), table.getTableName()).isPresent()) {
            throw new TableNotFoundException(table);
        }

        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, table);
        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        return new ConnectorTableMetadata(table, columns, createMetadataProperties(icebergTable), getTableComment(icebergTable));
    }

    public ExtendedHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(typeManager, constraint, handle, icebergTable);
    }

    private Optional<Long> getSnapshotId(org.apache.iceberg.Table table, Optional<Long> snapshotId)
    {
        if (snapshotId.isPresent()) {
            return Optional.of(IcebergUtil.resolveSnapshotId(table, snapshotId.get()));
        }
        return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    }
}
