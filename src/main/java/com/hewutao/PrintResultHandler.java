package com.hewutao;

import org.postgresql.core.BaseStatement;
import org.postgresql.core.Field;
import org.postgresql.core.Query;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

public class PrintResultHandler implements ResultHandler {
    private SQLException error;

    private BaseStatement originalStat;
    private String sql;

    public PrintResultHandler(String sql, Statement stat) {
        this.originalStat = (BaseStatement) stat;
        this.sql = sql;
    }

    @Override
    public void handleResultRows(Query fromQuery, Field[] fields, List tuples, ResultCursor cursor) {
        try
        {
            ResultSetImpl newResult = new ResultSetImpl(fromQuery, originalStat, fields, tuples, cursor,
                    originalStat.getMaxRows(), originalStat.getMaxFieldSize(),
                    originalStat.getResultSetType(), originalStat.getResultSetConcurrency(), originalStat.getResultSetHoldability());
            newResult.setFetchSize(originalStat.getFetchSize());
            newResult.setFetchDirection(originalStat.getFetchDirection());

            printResultSet(newResult);
        }
        catch (SQLException e)
        {
            handleError(e);
        }
    }

    @Override
    public void handleCommandStatus(String status, int updateCount, long insertOID) {
//        System.out.println("finish. status: " + status);
    }

    @Override
    public void handleWarning(SQLWarning warning) {
        System.out.println(warning);
    }

    @Override
    public void handleError(SQLException newError) {
        if (error == null)
            error = newError;
        else
            error.setNextException(newError);
    }

    @Override
    public void handleCompletion() throws SQLException {
        if (error != null)
            throw error;
    }

    private void printResultSet(ResultSet resultSet) throws SQLException {

        ResultSetMetaData metaData = resultSet.getMetaData();

        int count = metaData.getColumnCount();

        for (int i = 1; i <= count; i++) {
            if (i != 1) {
                System.out.print(" | ");
            }
            System.out.print(metaData.getColumnName(i));
        }
        System.out.println();
        System.out.println("------------------------");

        int len = 0;

        while (resultSet.next()) {
            for (int i = 1; i <= count; i++) {
                if (i != 1) {
                    System.out.print(" | ");
                }
                System.out.print(resultSet.getObject(i));
            }
            System.out.println();
            len++;
        }

        System.out.println(len + " rows");
    }
}
