package com.jdbcagent.core.protocol;

import java.io.Serializable;

/**
 * JDBC-Agent protocol DatabaseMetaDataMsg
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class DatabaseMetaDataMsg extends Message implements Serializable {
    private static final long serialVersionUID = -7253669370810164559L;

    private Long id;
    private Method method;
    private Serializable[] params;
    private Serializable response;

    public static Builder newBuilder() {
        return new Builder(new DatabaseMetaDataMsg());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public Serializable[] getParams() {
        return params;
    }

    public void setParams(Serializable[] params) {
        this.params = params;
    }

    public Serializable getResponse() {
        return response;
    }

    public void setResponse(Serializable response) {
        this.response = response;
    }

    public static class Builder {
        private DatabaseMetaDataMsg databaseMetaDataMsg;

        public Builder(DatabaseMetaDataMsg databaseMetaDataMsg) {
            this.databaseMetaDataMsg = databaseMetaDataMsg;
        }

        public Builder setId(Long id) {
            databaseMetaDataMsg.setId(id);
            return this;
        }

        public Builder setMethod(Method method) {
            databaseMetaDataMsg.setMethod(method);
            return this;
        }

        public Builder setParams(Serializable[] params) {
            databaseMetaDataMsg.setParams(params);
            return this;
        }

        public Builder setResponse(Serializable response) {
            databaseMetaDataMsg.setResponse(response);
            return this;
        }

        public DatabaseMetaDataMsg build() {
            return databaseMetaDataMsg;
        }
    }

    public enum Method {
        allProceduresAreCallable,
        allTablesAreSelectable,
        getURL,
        getUserName,
        isReadOnly,
        nullsAreSortedHigh,
        nullsAreSortedLow,
        nullsAreSortedAtStart,
        nullsAreSortedAtEnd,
        getDatabaseProductName,
        getDatabaseProductVersion,
        getDriverName,
        getDriverVersion,
        getDriverMajorVersion,
        getDriverMinorVersion,
        usesLocalFiles,
        usesLocalFilePerTable,
        supportsMixedCaseIdentifiers,
        storesUpperCaseIdentifiers,
        storesLowerCaseIdentifiers,
        storesMixedCaseIdentifiers,
        supportsMixedCaseQuotedIdentifiers,
        storesUpperCaseQuotedIdentifiers,
        storesLowerCaseQuotedIdentifiers,
        storesMixedCaseQuotedIdentifiers,
        getIdentifierQuoteString,
        getSQLKeywords,
        getNumericFunctions,
        getStringFunctions,
        getSystemFunctions,
        getTimeDateFunctions,
        getSearchStringEscape,
        getExtraNameCharacters,
        supportsAlterTableWithAddColumn,
        supportsAlterTableWithDropColumn,
        supportsColumnAliasing,
        nullPlusNonNullIsNull,
        supportsConvert,
        supportsTableCorrelationNames,
        supportsDifferentTableCorrelationNames,
        supportsExpressionsInOrderBy,
        supportsOrderByUnrelated,
        supportsGroupBy,
        supportsGroupByUnrelated,
        supportsGroupByBeyondSelect,
        supportsLikeEscapeClause,
        supportsMultipleResultSets,
        supportsMultipleTransactions,
        supportsNonNullableColumns,
        supportsMinimumSQLGrammar,
        supportsCoreSQLGrammar,
        supportsExtendedSQLGrammar,
        supportsANSI92EntryLevelSQL,
        supportsANSI92IntermediateSQL,
        supportsANSI92FullSQL,
        supportsIntegrityEnhancementFacility,
        supportsOuterJoins,
        supportsFullOuterJoins,
        supportsLimitedOuterJoins,
        getSchemaTerm,
        getProcedureTerm,
        getCatalogTerm,
        isCatalogAtStart,
        getCatalogSeparator,
        supportsSchemasInDataManipulation,
        supportsSchemasInProcedureCalls,
        supportsSchemasInTableDefinitions,
        supportsSchemasInIndexDefinitions,
        supportsSchemasInPrivilegeDefinitions,
        supportsCatalogsInDataManipulation,
        supportsCatalogsInProcedureCalls,
        supportsCatalogsInTableDefinitions,
        supportsCatalogsInIndexDefinitions,
        supportsCatalogsInPrivilegeDefinitions,
        supportsPositionedDelete,
        supportsPositionedUpdate,
        supportsSelectForUpdate,
        supportsStoredProcedures,
        supportsSubqueriesInComparisons,
        supportsSubqueriesInExists,
        supportsSubqueriesInIns,
        supportsSubqueriesInQuantifieds,
        supportsCorrelatedSubqueries,
        supportsUnion,
        supportsUnionAll,
        supportsOpenCursorsAcrossCommit,
        supportsOpenCursorsAcrossRollback,
        supportsOpenStatementsAcrossCommit,
        supportsOpenStatementsAcrossRollback,
        getMaxBinaryLiteralLength,
        getMaxCharLiteralLength,
        getMaxColumnNameLength,
        getMaxColumnsInGroupBy,
        getMaxColumnsInIndex,
        getMaxColumnsInOrderBy,
        getMaxColumnsInSelect,
        getMaxColumnsInTable,
        getMaxConnections,
        getMaxCursorNameLength,
        getMaxIndexLength,
        getMaxSchemaNameLength,
        getMaxProcedureNameLength,
        getMaxCatalogNameLength,
        getMaxRowSize,
        doesMaxRowSizeIncludeBlobs,
        getMaxStatementLength,
        getMaxStatements,
        getMaxTableNameLength,
        getMaxTablesInSelect,
        getMaxUserNameLength,
        getDefaultTransactionIsolation,
        supportsTransactions,
        supportsTransactionIsolationLevel,
        supportsDataDefinitionAndDataManipulationTransactions,
        supportsDataManipulationTransactionsOnly,
        dataDefinitionCausesTransactionCommit,
        dataDefinitionIgnoredInTransactions,
        getProcedures,
        getProcedureColumns,
        getTables,
        getSchemas,
        getCatalogs,
        getTableTypes,
        getColumns,
        getColumnPrivileges,
        getTablePrivileges,
        getBestRowIdentifier,
        getVersionColumns,
        getPrimaryKeys,
        getImportedKeys,
        getExportedKeys,
        getCrossReference,
        getTypeInfo,
        getIndexInfo,
        supportsResultSetType,
        supportsResultSetConcurrency,
        ownUpdatesAreVisible,
        ownDeletesAreVisible,
        ownInsertsAreVisible,
        othersUpdatesAreVisible,
        othersDeletesAreVisible,
        othersInsertsAreVisible,
        updatesAreDetected,
        deletesAreDetected,
        insertsAreDetected,
        supportsBatchUpdates,
        getUDTs,
        supportsSavepoints,
        supportsNamedParameters,
        supportsMultipleOpenResults,
        supportsGetGeneratedKeys,
        getSuperTypes,
        getSuperTables,
        getAttributes,
        supportsResultSetHoldability,
        getResultSetHoldability,
        getDatabaseMajorVersion,
        getDatabaseMinorVersion,
        getJDBCMajorVersion,
        getJDBCMinorVersion,
        getSQLStateType,
        locatorsUpdateCopy,
        supportsStatementPooling,
        getRowIdLifetime,
        supportsStoredFunctionsUsingCallSyntax,
        autoCommitFailureClosesAllResultSets,
        getClientInfoProperties,
        getFunctions,
        getFunctionColumns,
        getPseudoColumns,
        generatedKeyAlwaysReturned
    }
}
