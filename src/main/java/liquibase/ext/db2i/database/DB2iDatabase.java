package liquibase.ext.db2i.database;

import java.sql.ResultSet;
import java.sql.Statement;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.database.core.DB2Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.GetViewDefinitionStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.JdbcUtil;
import liquibase.util.StringUtil;

public class DB2iDatabase extends DB2Database {

    @Override
    public int getPriority() {
        return super.getPriority()+5;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getDatabaseProductName().startsWith("DB2 UDB for AS/400");
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:as400")) {
            return "com.ibm.as400.access.AS400JDBCDriver";
        }
        return null;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "DB2i";
    }

    @Override
    public String getShortName() {
        return "db2i";
    }
    
    @Override
    public String getDefaultCatalogName() {
        if (this.defaultCatalogName != null) {
            return this.defaultCatalogName;
        } else if (this.defaultSchemaName != null) {
            return this.defaultSchemaName;
        } else if (this.getConnection() == null) {
            return null;
        } else if (this.getConnection() instanceof OfflineConnection) {
            return ((OfflineConnection)this.getConnection()).getSchema();
        } else {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = ((JdbcConnection)this.getConnection()).createStatement();
                rs = stmt.executeQuery("SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1");
                if (rs.next()) {
                    String result = rs.getString(1);
                    if (result != null) {
                        this.defaultSchemaName = StringUtil.trimToNull(result);
                    } else {
                        this.defaultSchemaName = StringUtil.trimToNull(super.getDefaultSchemaName());
                    }
                }
            } catch (Exception var7) {
                throw new RuntimeException("Could not determine current schema", var7);
            } finally {
                JdbcUtil.close(rs, stmt);
            }

            return this.defaultSchemaName;
        }
    }

    @Override
	public boolean supportsBooleanDataType() {
        if (getConnection() == null)
            return false;
        try {
            final Integer countBooleanType = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).queryForObject(
                    new RawSqlStatement("select count(*) from sysibm.sqltypeinfo where type_name = 'BOOLEAN'"),
                    Integer.class);
            return countBooleanType == 1;
        } catch (final Exception e) {
            Scope.getCurrentScope().getLog(getClass()).info("Error checking for BOOLEAN type", e);
        }
        return false;
    }
    
    @Override
    public String getViewDefinition(CatalogAndSchema schema, final String viewName) throws DatabaseException {
        schema = schema.customize(this);
        String definition = Scope.getCurrentScope().getSingleton(ExecutorService.class)
        		.getExecutor("jdbc", this)
        		.queryForObject(new GetViewDefinitionStatement(schema.getCatalogName(), schema.getSchemaName(), viewName), String.class);
        if (definition == null) {
            return null;
        }
        return definition;
    }
}
