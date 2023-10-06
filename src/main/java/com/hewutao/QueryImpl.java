package com.hewutao;

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;

public class QueryImpl implements Query {
    private final String sql;

    public QueryImpl(String sql) {
        this.sql = sql;
    }

    @Override
    public ParameterList createParameterList() {
        return null;
    }

    @Override
    public String toString(ParameterList parameters) {
        return sql;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isStatementDescribed() {
        return false;
    }
}
