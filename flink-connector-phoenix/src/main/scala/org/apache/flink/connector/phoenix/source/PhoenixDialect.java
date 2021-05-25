package org.apache.flink.connector.phoenix.source;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.phoenix.Constans;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

/**
 * PhoenixDialect
 */
public class PhoenixDialect implements JdbcDialect {

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(Constans.PHOENIX_DRIVER);
    }

    @Override
    public String dialectName() {
        return "Phoenix";
    }

    @Override
    public boolean canHandle(String s) {
        return s.startsWith("http");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new PhoenixRowConverter(rowType);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }
}
