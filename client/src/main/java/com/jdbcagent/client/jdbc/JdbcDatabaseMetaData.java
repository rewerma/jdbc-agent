package com.jdbcagent.client.jdbc;

import com.jdbcagent.client.JdbcAgentConnector;
import com.jdbcagent.core.protocol.DatabaseMetaDataMsg;
import com.jdbcagent.core.protocol.DatabaseMetaDataMsg.Method;
import com.jdbcagent.core.protocol.Packet;
import com.jdbcagent.core.protocol.Packet.PacketType;

import java.io.Serializable;
import java.sql.*;

/**
 * JDBC-Agent client jdbc databaseMeta impl
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class JdbcDatabaseMetaData implements DatabaseMetaData {

    private long remoteId;                                  // 远程dataMetaData id

    private Connection conn;                                // connection

    private final JdbcAgentConnector jdbcAgentConnector;    // tcp连接器

    /**
     * 构造函数
     *
     * @param conn               connection
     * @param jdbcAgentConnector tcp连接器
     * @param remoteId           远程dataMetaData id
     */
    JdbcDatabaseMetaData(Connection conn, JdbcAgentConnector jdbcAgentConnector, long remoteId) {
        this.conn = conn;
        this.remoteId = remoteId;
        this.jdbcAgentConnector = jdbcAgentConnector;
    }

    /**
     * CallableStatement方法远程调用, 无参数
     *
     * @param method 方法名
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokeCallableStatementMethod(Method method) throws SQLException {
        return invokeCallableStatementMethod(method, new Serializable[0]);
    }

    /**
     * CallableStatement方法远程调用, 带参数
     *
     * @param method 方法名
     * @param params 方法参数
     * @return 可序列化返回值
     * @throws SQLException
     */
    private Serializable invokeCallableStatementMethod(Method method, Serializable... params)
            throws SQLException {
        synchronized (jdbcAgentConnector) {
            Packet responsePacket =
                    Packet.parse(
                            jdbcAgentConnector.write(
                                    Packet.newBuilder()
                                            .incrementAndGetId()
                                            .setType(PacketType.DB_METADATA_METHOD)
                                            .setBody(DatabaseMetaDataMsg.newBuilder().setId(remoteId)
                                                    .setMethod(method).setParams(params).build())
                                            .build()));
            return ((DatabaseMetaDataMsg) responsePacket.getBody()).getResponse();
        }
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.allProceduresAreCallable);
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.allTablesAreSelectable);
    }

    @Override
    public String getURL() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getURL);
    }

    @Override
    public String getUserName() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getUserName);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.isReadOnly);
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.nullsAreSortedHigh);
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.nullsAreSortedLow);
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.nullsAreSortedAtStart);
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.nullsAreSortedAtEnd);
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getDatabaseProductName);
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getDatabaseProductVersion);
    }

    @Override
    public String getDriverName() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getDriverName);
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getDriverVersion);
    }

    @Override
    public int getDriverMajorVersion() {
        try {
            return (Integer) invokeCallableStatementMethod(Method.getDriverMajorVersion);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getDriverMinorVersion() {
        try {
            return (Integer) invokeCallableStatementMethod(Method.getDriverMinorVersion);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.usesLocalFiles);
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.usesLocalFilePerTable);
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsMixedCaseIdentifiers);
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.storesUpperCaseIdentifiers);
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.storesLowerCaseIdentifiers);
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.storesMixedCaseIdentifiers);
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsMixedCaseQuotedIdentifiers);
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.storesUpperCaseQuotedIdentifiers);
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.storesLowerCaseQuotedIdentifiers);
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.storesMixedCaseQuotedIdentifiers);
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getIdentifierQuoteString);
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getSQLKeywords);
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getNumericFunctions);
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getStringFunctions);
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getSystemFunctions);
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getTimeDateFunctions);
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getSearchStringEscape);
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getExtraNameCharacters);
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsAlterTableWithAddColumn);
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsAlterTableWithDropColumn);
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsColumnAliasing);
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.nullPlusNonNullIsNull);
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsConvert);
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsConvert, fromType, toType);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsTableCorrelationNames);
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsDifferentTableCorrelationNames);
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsExpressionsInOrderBy);
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsOrderByUnrelated);
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsGroupBy);
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsGroupByUnrelated);
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsGroupByBeyondSelect);
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsLikeEscapeClause);
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsMultipleResultSets);
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsMultipleTransactions);
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsNonNullableColumns);
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsMinimumSQLGrammar);
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCoreSQLGrammar);
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsExtendedSQLGrammar);
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsANSI92EntryLevelSQL);
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsANSI92IntermediateSQL);
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsANSI92FullSQL);
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsIntegrityEnhancementFacility);
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsOuterJoins);
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsFullOuterJoins);
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsLimitedOuterJoins);
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getSchemaTerm);
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getProcedureTerm);
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getCatalogTerm);
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.isCatalogAtStart);
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return (String) invokeCallableStatementMethod(Method.getCatalogSeparator);
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSchemasInDataManipulation);
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSchemasInProcedureCalls);
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSchemasInTableDefinitions);
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSchemasInIndexDefinitions);
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSchemasInPrivilegeDefinitions);
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCatalogsInDataManipulation);
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCatalogsInProcedureCalls);
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCatalogsInTableDefinitions);
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCatalogsInIndexDefinitions);
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCatalogsInPrivilegeDefinitions);
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsPositionedDelete);
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsPositionedUpdate);
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSelectForUpdate);
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsStoredProcedures);
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSubqueriesInComparisons);
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSubqueriesInExists);
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSubqueriesInIns);
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSubqueriesInQuantifieds);
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsCorrelatedSubqueries);
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsUnion);
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsUnionAll);
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsOpenCursorsAcrossCommit);
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsOpenCursorsAcrossRollback);
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsOpenStatementsAcrossCommit);
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsOpenStatementsAcrossRollback);
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxBinaryLiteralLength);
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxCharLiteralLength);
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxColumnNameLength);
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxColumnsInGroupBy);
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxColumnsInIndex);
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxColumnsInOrderBy);
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxColumnsInSelect);
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxColumnsInTable);
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxConnections);
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxCursorNameLength);
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxIndexLength);
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxSchemaNameLength);
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxProcedureNameLength);
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxCatalogNameLength);
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxRowSize);
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.doesMaxRowSizeIncludeBlobs);
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxStatementLength);
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxStatements);
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxTableNameLength);
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxTablesInSelect);
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getMaxUserNameLength);
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getDefaultTransactionIsolation);
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsTransactions);
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsTransactionIsolationLevel);
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsDataDefinitionAndDataManipulationTransactions);
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsDataManipulationTransactionsOnly);
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.dataDefinitionCausesTransactionCommit);
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.dataDefinitionIgnoredInTransactions);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        Long rsId = (Long) invokeCallableStatementMethod(Method.getProcedures, catalog, schemaPattern, procedureNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, rsId);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern) throws SQLException {
        Long rsId = (Long) invokeCallableStatementMethod(Method.getProcedureColumns, catalog, schemaPattern,
                procedureNamePattern, columnNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, rsId);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        Long rsId = (Long) invokeCallableStatementMethod(Method.getTables, catalog, schemaPattern,
                tableNamePattern, types);
        return new JdbcResultSet(jdbcAgentConnector, rsId);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Long rsId = (Long) invokeCallableStatementMethod(Method.getSchemas);
        return new JdbcResultSet(jdbcAgentConnector, rsId);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getCatalogs);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getTableTypes);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getColumns, catalog, schemaPattern,
                tableNamePattern, columnNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getColumnPrivileges, catalog, schema,
                table, columnNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getTablePrivileges, catalog, schemaPattern,
                tableNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          boolean nullable) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getBestRowIdentifier, catalog, schema,
                table, scope, nullable);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getVersionColumns, catalog, schema, table);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getPrimaryKeys, catalog, schema, table);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getImportedKeys, catalog, schema, table);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getExportedKeys, catalog, schema, table);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                       String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getCrossReference, parentCatalog, parentCatalog,
                parentTable, foreignCatalog, foreignSchema, foreignTable);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getTypeInfo);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getIndexInfo, catalog, schema, table,
                unique, approximate);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsResultSetType, type);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsResultSetConcurrency, type, concurrency);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.ownUpdatesAreVisible, type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.ownDeletesAreVisible, type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.ownInsertsAreVisible, type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.othersUpdatesAreVisible, type);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.othersDeletesAreVisible, type);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.othersInsertsAreVisible, type);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.updatesAreDetected, type);
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.deletesAreDetected, type);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.insertsAreDetected, type);
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsBatchUpdates);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                             int[] types) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getUDTs, catalog, schemaPattern,
                typeNamePattern, types);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSavepoints);
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsSavepoints);
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsMultipleOpenResults);
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsGetGeneratedKeys);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getSuperTypes, catalog, schemaPattern,
                typeNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getSuperTables, catalog, schemaPattern,
                tableNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getAttributes, catalog, schemaPattern,
                attributeNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsResultSetHoldability, holdability);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getResultSetHoldability);
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getDatabaseMajorVersion);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getDatabaseMinorVersion);
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getJDBCMajorVersion);
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getJDBCMinorVersion);
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return (Integer) invokeCallableStatementMethod(Method.getSQLStateType);
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.locatorsUpdateCopy);
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsStatementPooling);
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return (RowIdLifetime) invokeCallableStatementMethod(Method.getRowIdLifetime);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getSchemas, catalog, schemaPattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.supportsStoredFunctionsUsingCallSyntax);
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.autoCommitFailureClosesAllResultSets);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getClientInfoProperties);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getFunctions, catalog, schemaPattern, functionNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getFunctionColumns, catalog, schemaPattern,
                functionNamePattern, columnNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        long resId = (Long) invokeCallableStatementMethod(Method.getPseudoColumns, catalog, schemaPattern, columnNamePattern);
        return new JdbcResultSet(jdbcAgentConnector, resId);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return (Boolean) invokeCallableStatementMethod(Method.generatedKeyAlwaysReturned);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
