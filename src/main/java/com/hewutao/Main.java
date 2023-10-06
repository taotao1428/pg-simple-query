package com.hewutao;

import org.postgresql.core.ResultHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://xx.xx.xx.xx:5432/postgres";
        String user = "xxx";
        String password = "xxxx";
        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            try (Statement stat = conn.createStatement()) {
                String sql = "select * from person_tbl; \ninsert into person_tbl values ('" + UUID.randomUUID().toString() + "', 'hwt'); drop table if exists test_xxx;";

                QueryExecutorWrapper wrapper = new QueryExecutorWrapper(conn, stat);
                wrapper.sendSimpleQuery(sql);

                ResultHandler handler = new PrintResultHandler(sql, stat);

                wrapper.processResults(handler, 0);

                handler.handleCompletion();
            }
        }
    }
}