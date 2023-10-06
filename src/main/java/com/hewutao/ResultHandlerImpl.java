package com.hewutao;

import org.postgresql.core.BaseStatement;
import org.postgresql.core.Field;
import org.postgresql.core.Query;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandler;
import org.postgresql.jdbc2.ResultWrapper;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

public class ResultHandlerImpl implements ResultHandler {
    private SQLException error;
    private ResultWrapper results;
    private List<SQLWarning> warnings;

    private BaseStatement originalStat;
    private String sql;

    public ResultHandlerImpl(String sql, Statement stat) {
        this.originalStat = (BaseStatement) stat;
        this.sql = sql;
    }

    ResultWrapper getResults() {
        return results;
    }

    private void append(ResultWrapper newResult) {
        if (results == null)
            results = newResult;
        else
            results.append(newResult);
    }

    public void handleResultRows(Query fromQuery, Field[] fields, List tuples, ResultCursor cursor) {
        try
        {
            ResultSetImpl newResult = new ResultSetImpl(fromQuery, originalStat, fields, tuples, cursor,
                    originalStat.getMaxRows(), originalStat.getMaxFieldSize(),
                    originalStat.getResultSetType(), originalStat.getResultSetConcurrency(), originalStat.getResultSetHoldability());
            newResult.setFetchSize(originalStat.getFetchSize());
            newResult.setFetchDirection(originalStat.getFetchDirection());
            append(new ResultWrapper(newResult));
        }
        catch (SQLException e)
        {
            handleError(e);
        }
    }

    public void handleCommandStatus(String status, int updateCount, long insertOID) {
        append(new ResultWrapper(updateCount, insertOID));
    }

    public void handleWarning(SQLWarning warning) {
        this.warnings.add(warning);
    }

    public void handleError(SQLException newError) {
        if (error == null)
            error = newError;
        else
            error.setNextException(newError);
    }

    public void handleCompletion() throws SQLException {
        if (error != null)
            throw error;
    }
}
