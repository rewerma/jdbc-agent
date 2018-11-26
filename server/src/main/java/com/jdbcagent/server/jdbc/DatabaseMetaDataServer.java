package com.jdbcagent.server.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jdbcagent.core.protocol.DatabaseMetaDataMsg;
import com.jdbcagent.core.protocol.DatabaseMetaDataMsg.Method;
import com.jdbcagent.core.support.serial.SerialVoid;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC-Agent server 端 databaseMetaData 操作类
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DatabaseMetaDataServer {
    private static AtomicLong DB_META_DATA_ID = new AtomicLong(0);      // id与client对应

    public static Cache<Long, DatabaseMetaDataServer> DB_META_DATAS = Caffeine.newBuilder()
            .expireAfterAccess(3, TimeUnit.MINUTES)
            .maximumSize(1000000)
            .build();                                                   // databaseMetaDataServer 缓存, 保存3分钟自动删除

    long currentId;                                                     // 当前id

    private DatabaseMetaData databaseMetaData;                          // 实际调用的databaseMetaData

    DatabaseMetaDataServer(DatabaseMetaData databaseMetaData) {
        currentId = DB_META_DATA_ID.incrementAndGet();
        this.databaseMetaData = databaseMetaData;
        DB_META_DATAS.put(currentId, this);
    }

    /**
     * databaseMetaData 公共方法调用
     *
     * @param databaseMetaDataMsg 调用信息
     * @return 返回结果
     * @throws SQLException
     */
    public Serializable databaseMetaDataMethod(DatabaseMetaDataMsg databaseMetaDataMsg) throws SQLException {
        try {
            Serializable response = new SerialVoid();
            Method method = databaseMetaDataMsg.getMethod();
            Serializable[] params = databaseMetaDataMsg.getParams();
            switch (method) {
                case allProceduresAreCallable:
                    response = databaseMetaData.allProceduresAreCallable();
                    break;
                case allTablesAreSelectable:
                    response = databaseMetaData.allTablesAreSelectable();
                    break;
                case getURL:
                    response = databaseMetaData.getURL();
                    break;
                case getUserName:
                    response = databaseMetaData.getUserName();
                    break;
                case isReadOnly:
                    response = databaseMetaData.isReadOnly();
                    break;
                case nullsAreSortedHigh:
                    response = databaseMetaData.nullsAreSortedHigh();
                    break;
                case nullsAreSortedAtStart:
                    response = databaseMetaData.nullsAreSortedAtStart();
                    break;
                case nullsAreSortedAtEnd:
                    response = databaseMetaData.nullsAreSortedAtEnd();
                    break;
                case getDatabaseProductName:
                    response = databaseMetaData.getDatabaseProductName();
                    break;
                case getDatabaseProductVersion:
                    response = databaseMetaData.getDatabaseProductVersion();
                    break;
                case getDriverName:
                    response = databaseMetaData.getDriverName();
                    break;
                case getDriverVersion:
                    response = databaseMetaData.getDriverVersion();
                    break;
                case getDriverMajorVersion:
                    response = databaseMetaData.getDriverMajorVersion();
                    break;
                case getDriverMinorVersion:
                    response = databaseMetaData.getURL();
                    break;
                case usesLocalFiles:
                    response = databaseMetaData.usesLocalFiles();
                    break;
                case usesLocalFilePerTable:
                    response = databaseMetaData.usesLocalFilePerTable();
                    break;
                case supportsMixedCaseIdentifiers:
                    response = databaseMetaData.supportsMixedCaseIdentifiers();
                    break;
                case storesUpperCaseIdentifiers:
                    response = databaseMetaData.storesUpperCaseIdentifiers();
                    break;
                case storesLowerCaseIdentifiers:
                    response = databaseMetaData.storesLowerCaseIdentifiers();
                    break;
                case storesMixedCaseIdentifiers:
                    response = databaseMetaData.storesMixedCaseIdentifiers();
                    break;
                case supportsMixedCaseQuotedIdentifiers:
                    response = databaseMetaData.supportsMixedCaseQuotedIdentifiers();
                    break;
                case storesUpperCaseQuotedIdentifiers:
                    response = databaseMetaData.storesUpperCaseQuotedIdentifiers();
                    break;
                case storesLowerCaseQuotedIdentifiers:
                    response = databaseMetaData.storesLowerCaseQuotedIdentifiers();
                    break;
                case storesMixedCaseQuotedIdentifiers:
                    response = databaseMetaData.storesMixedCaseQuotedIdentifiers();
                    break;
                case getIdentifierQuoteString:
                    response = databaseMetaData.getIdentifierQuoteString();
                    break;
                case getSQLKeywords:
                    response = databaseMetaData.getSQLKeywords();
                    break;
                case getNumericFunctions:
                    response = databaseMetaData.getNumericFunctions();
                    break;
                case getStringFunctions:
                    response = databaseMetaData.getStringFunctions();
                    break;
                case getSystemFunctions:
                    response = databaseMetaData.getSystemFunctions();
                    break;
                case getTimeDateFunctions:
                    response = databaseMetaData.getTimeDateFunctions();
                    break;
                case getSearchStringEscape:
                    response = databaseMetaData.getSearchStringEscape();
                    break;
                case getExtraNameCharacters:
                    response = databaseMetaData.getExtraNameCharacters();
                    break;
                case supportsAlterTableWithAddColumn:
                    response = databaseMetaData.supportsAlterTableWithAddColumn();
                    break;
                case supportsAlterTableWithDropColumn:
                    response = databaseMetaData.supportsAlterTableWithDropColumn();
                    break;
                case supportsColumnAliasing:
                    response = databaseMetaData.supportsColumnAliasing();
                    break;
                case nullPlusNonNullIsNull:
                    response = databaseMetaData.nullPlusNonNullIsNull();
                    break;
                case supportsConvert:
                    response = databaseMetaData.supportsConvert();
                    break;
                case supportsTableCorrelationNames:
                    response = databaseMetaData.supportsTableCorrelationNames();
                    break;
                case supportsDifferentTableCorrelationNames:
                    response = databaseMetaData.supportsDifferentTableCorrelationNames();
                    break;
                case supportsExpressionsInOrderBy:
                    response = databaseMetaData.supportsExpressionsInOrderBy();
                    break;
                case supportsOrderByUnrelated:
                    response = databaseMetaData.supportsOrderByUnrelated();
                    break;
                case supportsGroupBy:
                    response = databaseMetaData.supportsGroupBy();
                    break;
                case supportsGroupByUnrelated:
                    response = databaseMetaData.supportsGroupByUnrelated();
                    break;
                case supportsGroupByBeyondSelect:
                    response = databaseMetaData.supportsGroupByBeyondSelect();
                    break;
                case supportsLikeEscapeClause:
                    response = databaseMetaData.supportsLikeEscapeClause();
                    break;
                case supportsMultipleResultSets:
                    response = databaseMetaData.supportsMultipleResultSets();
                    break;
                case supportsMultipleTransactions:
                    response = databaseMetaData.supportsMultipleTransactions();
                    break;
                case supportsNonNullableColumns:
                    response = databaseMetaData.supportsNonNullableColumns();
                    break;
                case supportsMinimumSQLGrammar:
                    response = databaseMetaData.supportsMinimumSQLGrammar();
                    break;
                case supportsCoreSQLGrammar:
                    response = databaseMetaData.supportsCoreSQLGrammar();
                    break;
                case supportsExtendedSQLGrammar:
                    response = databaseMetaData.supportsExtendedSQLGrammar();
                    break;
                case supportsANSI92EntryLevelSQL:
                    response = databaseMetaData.supportsANSI92EntryLevelSQL();
                    break;
                case supportsANSI92IntermediateSQL:
                    response = databaseMetaData.supportsANSI92IntermediateSQL();
                    break;
                case supportsANSI92FullSQL:
                    response = databaseMetaData.supportsANSI92FullSQL();
                    break;
                case supportsIntegrityEnhancementFacility:
                    response = databaseMetaData.supportsIntegrityEnhancementFacility();
                    break;
                case supportsOuterJoins:
                    response = databaseMetaData.supportsOuterJoins();
                    break;
                case supportsFullOuterJoins:
                    response = databaseMetaData.supportsFullOuterJoins();
                    break;
                case supportsLimitedOuterJoins:
                    response = databaseMetaData.supportsLimitedOuterJoins();
                    break;
                case getSchemaTerm:
                    response = databaseMetaData.getSchemaTerm();
                    break;
                case getProcedureTerm:
                    response = databaseMetaData.getProcedureTerm();
                    break;
                case getCatalogTerm:
                    response = databaseMetaData.getCatalogTerm();
                    break;
                case isCatalogAtStart:
                    response = databaseMetaData.isCatalogAtStart();
                    break;
                case getCatalogSeparator:
                    response = databaseMetaData.getCatalogSeparator();
                    break;
                case supportsSchemasInDataManipulation:
                    response = databaseMetaData.supportsSchemasInDataManipulation();
                    break;
                case supportsSchemasInProcedureCalls:
                    response = databaseMetaData.supportsSchemasInProcedureCalls();
                    break;
                case supportsSchemasInTableDefinitions:
                    response = databaseMetaData.supportsSchemasInTableDefinitions();
                    break;
                case supportsSchemasInIndexDefinitions:
                    response = databaseMetaData.supportsSchemasInIndexDefinitions();
                    break;
                case supportsSchemasInPrivilegeDefinitions:
                    response = databaseMetaData.supportsSchemasInPrivilegeDefinitions();
                    break;
                case supportsCatalogsInDataManipulation:
                    response = databaseMetaData.supportsCatalogsInDataManipulation();
                    break;
                case supportsCatalogsInProcedureCalls:
                    response = databaseMetaData.supportsCatalogsInProcedureCalls();
                    break;
                case supportsCatalogsInTableDefinitions:
                    response = databaseMetaData.supportsCatalogsInTableDefinitions();
                    break;
                case supportsCatalogsInIndexDefinitions:
                    response = databaseMetaData.supportsCatalogsInIndexDefinitions();
                    break;
                case supportsCatalogsInPrivilegeDefinitions:
                    response = databaseMetaData.supportsCatalogsInPrivilegeDefinitions();
                    break;
                case supportsPositionedDelete:
                    response = databaseMetaData.supportsPositionedDelete();
                    break;
                case supportsPositionedUpdate:
                    response = databaseMetaData.supportsPositionedUpdate();
                    break;
                case supportsSelectForUpdate:
                    response = databaseMetaData.supportsSelectForUpdate();
                    break;
                case supportsStoredProcedures:
                    response = databaseMetaData.supportsStoredProcedures();
                    break;
                case supportsSubqueriesInComparisons:
                    response = databaseMetaData.supportsSubqueriesInComparisons();
                    break;
                case supportsSubqueriesInExists:
                    response = databaseMetaData.supportsSubqueriesInExists();
                    break;
                case supportsSubqueriesInIns:
                    response = databaseMetaData.supportsSubqueriesInIns();
                    break;
                case supportsSubqueriesInQuantifieds:
                    response = databaseMetaData.supportsSubqueriesInQuantifieds();
                    break;
                case supportsCorrelatedSubqueries:
                    response = databaseMetaData.supportsCorrelatedSubqueries();
                    break;
                case supportsUnion:
                    response = databaseMetaData.supportsUnion();
                    break;
                case supportsUnionAll:
                    response = databaseMetaData.supportsUnionAll();
                    break;
                case supportsOpenCursorsAcrossCommit:
                    response = databaseMetaData.supportsOpenCursorsAcrossCommit();
                    break;
                case supportsOpenCursorsAcrossRollback:
                    response = databaseMetaData.supportsOpenCursorsAcrossRollback();
                    break;
                case supportsOpenStatementsAcrossCommit:
                    response = databaseMetaData.supportsOpenStatementsAcrossCommit();
                    break;
                case supportsOpenStatementsAcrossRollback:
                    response = databaseMetaData.supportsOpenStatementsAcrossRollback();
                    break;
                case getMaxBinaryLiteralLength:
                    response = databaseMetaData.getMaxBinaryLiteralLength();
                    break;
                case getMaxCharLiteralLength:
                    response = databaseMetaData.getMaxCharLiteralLength();
                    break;
                case getMaxColumnNameLength:
                    response = databaseMetaData.getMaxColumnNameLength();
                    break;
                case getMaxColumnsInGroupBy:
                    response = databaseMetaData.getMaxColumnsInGroupBy();
                    break;
                case getMaxColumnsInIndex:
                    response = databaseMetaData.getMaxColumnsInIndex();
                    break;
                case getMaxColumnsInOrderBy:
                    response = databaseMetaData.getMaxColumnsInOrderBy();
                    break;
                case getMaxColumnsInSelect:
                    response = databaseMetaData.getMaxColumnsInSelect();
                    break;
                case getMaxColumnsInTable:
                    response = databaseMetaData.getMaxColumnsInTable();
                    break;
                case getMaxConnections:
                    response = databaseMetaData.getMaxConnections();
                    break;
                case getMaxCursorNameLength:
                    response = databaseMetaData.getMaxCursorNameLength();
                    break;
                case getMaxIndexLength:
                    response = databaseMetaData.getMaxIndexLength();
                    break;
                case getMaxSchemaNameLength:
                    response = databaseMetaData.getMaxSchemaNameLength();
                    break;
                case getMaxProcedureNameLength:
                    response = databaseMetaData.getMaxProcedureNameLength();
                    break;
                case getMaxCatalogNameLength:
                    response = databaseMetaData.getMaxCatalogNameLength();
                    break;
                case getMaxRowSize:
                    response = databaseMetaData.getMaxRowSize();
                    break;
                case doesMaxRowSizeIncludeBlobs:
                    response = databaseMetaData.doesMaxRowSizeIncludeBlobs();
                    break;
                case getMaxStatementLength:
                    response = databaseMetaData.getMaxStatementLength();
                    break;
                case getMaxStatements:
                    response = databaseMetaData.getMaxStatements();
                    break;
                case getMaxTableNameLength:
                    response = databaseMetaData.getMaxTableNameLength();
                    break;
                case getMaxTablesInSelect:
                    response = databaseMetaData.getMaxTablesInSelect();
                    break;
                case getDefaultTransactionIsolation:
                    response = databaseMetaData.getDefaultTransactionIsolation();
                    break;
                case supportsTransactions:
                    response = databaseMetaData.supportsTransactions();
                    break;
                case supportsTransactionIsolationLevel:
                    response = databaseMetaData.supportsTransactionIsolationLevel((Integer) params[0]);
                    break;
                case supportsDataDefinitionAndDataManipulationTransactions:
                    response = databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions();
                    break;
                case supportsDataManipulationTransactionsOnly:
                    response = databaseMetaData.supportsDataManipulationTransactionsOnly();
                    break;
                case dataDefinitionCausesTransactionCommit:
                    response = databaseMetaData.dataDefinitionCausesTransactionCommit();
                    break;
                case dataDefinitionIgnoredInTransactions:
                    response = databaseMetaData.dataDefinitionIgnoredInTransactions();
                    break;
                case getProcedures:
                    response = new ResultSetServer(databaseMetaData.getProcedures((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getProcedureColumns:
                    response = new ResultSetServer(databaseMetaData.getProcedureColumns((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3])).currentId;
                    break;
                case getTables:
                    response = new ResultSetServer(databaseMetaData.getTables((String) params[0],
                            (String) params[1], (String) params[2], (String[]) params[3])).currentId;
                    break;
                case getSchemas:
                    response = new ResultSetServer(databaseMetaData.getSchemas()).currentId;
                    break;
                case getCatalogs:
                    response = new ResultSetServer(databaseMetaData.getCatalogs()).currentId;
                    break;
                case getTableTypes:
                    response = new ResultSetServer(databaseMetaData.getTableTypes()).currentId;
                    break;
                case getColumns:
                    response = new ResultSetServer(databaseMetaData.getColumns((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3])).currentId;
                    break;
                case getColumnPrivileges:
                    response = new ResultSetServer(databaseMetaData.getColumnPrivileges((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3])).currentId;
                    break;
                case getTablePrivileges:
                    response = new ResultSetServer(databaseMetaData.getTablePrivileges((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getBestRowIdentifier:
                    response = new ResultSetServer(databaseMetaData.getBestRowIdentifier((String) params[0],
                            (String) params[1], (String) params[2], (Integer) params[3], (Boolean) params[4])).currentId;
                    break;
                case getVersionColumns:
                    response = new ResultSetServer(databaseMetaData.getVersionColumns((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getPrimaryKeys:
                    response = new ResultSetServer(databaseMetaData.getPrimaryKeys((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getImportedKeys:
                    response = new ResultSetServer(databaseMetaData.getImportedKeys((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getExportedKeys:
                    response = new ResultSetServer(databaseMetaData.getExportedKeys((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getCrossReference:
                    response = new ResultSetServer(databaseMetaData.getCrossReference((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3],
                            (String) params[4], (String) params[5])).currentId;
                    break;
                case getTypeInfo:
                    response = new ResultSetServer(databaseMetaData.getTypeInfo()).currentId;
                    break;
                case getIndexInfo:
                    response = new ResultSetServer(databaseMetaData.getIndexInfo((String) params[0],
                            (String) params[1], (String) params[2], (Boolean) params[3], (Boolean) params[4])).currentId;
                    break;
                case supportsResultSetType:
                    response = databaseMetaData.supportsResultSetType((Integer) params[0]);
                    break;
                case supportsResultSetConcurrency:
                    response = databaseMetaData.supportsResultSetConcurrency((Integer) params[0], (Integer) params[1]);
                    break;
                case ownUpdatesAreVisible:
                    response = databaseMetaData.ownUpdatesAreVisible((Integer) params[0]);
                    break;
                case ownDeletesAreVisible:
                    response = databaseMetaData.ownDeletesAreVisible((Integer) params[0]);
                    break;
                case ownInsertsAreVisible:
                    response = databaseMetaData.ownInsertsAreVisible((Integer) params[0]);
                    break;
                case othersUpdatesAreVisible:
                    response = databaseMetaData.othersUpdatesAreVisible((Integer) params[0]);
                    break;
                case othersDeletesAreVisible:
                    response = databaseMetaData.othersDeletesAreVisible((Integer) params[0]);
                    break;
                case othersInsertsAreVisible:
                    response = databaseMetaData.othersInsertsAreVisible((Integer) params[0]);
                    break;
                case updatesAreDetected:
                    response = databaseMetaData.updatesAreDetected((Integer) params[0]);
                    break;
                case deletesAreDetected:
                    response = databaseMetaData.deletesAreDetected((Integer) params[0]);
                    break;
                case insertsAreDetected:
                    response = databaseMetaData.insertsAreDetected((Integer) params[0]);
                    break;
                case supportsBatchUpdates:
                    response = databaseMetaData.supportsBatchUpdates();
                    break;
                case getUDTs:
                    response = new ResultSetServer(databaseMetaData.getUDTs((String) params[0],
                            (String) params[1], (String) params[2], (int[]) params[3])).currentId;
                    break;
                case supportsSavepoints:
                    response = databaseMetaData.supportsSavepoints();
                    break;
                case supportsNamedParameters:
                    response = databaseMetaData.supportsNamedParameters();
                    break;
                case supportsMultipleOpenResults:
                    response = databaseMetaData.supportsMultipleOpenResults();
                    break;
                case supportsGetGeneratedKeys:
                    response = databaseMetaData.supportsGetGeneratedKeys();
                    break;
                case getSuperTypes:
                    response = new ResultSetServer(databaseMetaData.getSuperTypes((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getSuperTables:
                    response = new ResultSetServer(databaseMetaData.getSuperTables((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getAttributes:
                    response = new ResultSetServer(databaseMetaData.getAttributes((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3])).currentId;
                    break;
                case supportsResultSetHoldability:
                    response = databaseMetaData.supportsResultSetHoldability((Integer) params[0]);
                    break;
                case getResultSetHoldability:
                    response = databaseMetaData.getResultSetHoldability();
                    break;
                case getDatabaseMajorVersion:
                    response = databaseMetaData.getDatabaseMajorVersion();
                    break;
                case getDatabaseMinorVersion:
                    response = databaseMetaData.getDatabaseMinorVersion();
                    break;
                case getJDBCMajorVersion:
                    response = databaseMetaData.getJDBCMajorVersion();
                    break;
                case getJDBCMinorVersion:
                    response = databaseMetaData.getJDBCMinorVersion();
                    break;
                case getSQLStateType:
                    response = databaseMetaData.getSQLStateType();
                    break;
                case locatorsUpdateCopy:
                    response = databaseMetaData.locatorsUpdateCopy();
                    break;
                case supportsStatementPooling:
                    response = databaseMetaData.supportsStatementPooling();
                    break;
                case getRowIdLifetime:
                    response = databaseMetaData.getRowIdLifetime();
                    break;
                case supportsStoredFunctionsUsingCallSyntax:
                    response = databaseMetaData.supportsStoredFunctionsUsingCallSyntax();
                    break;
                case autoCommitFailureClosesAllResultSets:
                    response = databaseMetaData.autoCommitFailureClosesAllResultSets();
                    break;
                case getClientInfoProperties:
                    response = new ResultSetServer(databaseMetaData.getClientInfoProperties()).currentId;
                    break;
                case getFunctions:
                    response = new ResultSetServer(databaseMetaData.getFunctions((String) params[0],
                            (String) params[1], (String) params[2])).currentId;
                    break;
                case getFunctionColumns:
                    response = new ResultSetServer(databaseMetaData.getFunctionColumns((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3])).currentId;
                    break;
                case getPseudoColumns:
                    response = new ResultSetServer(databaseMetaData.getPseudoColumns((String) params[0],
                            (String) params[1], (String) params[2], (String) params[3])).currentId;
                    break;
                case generatedKeyAlwaysReturned:
                    response = databaseMetaData.generatedKeyAlwaysReturned();
                    break;
            }
            return response;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
