package org.apache.flink.connector.phoenix;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

public class DataSource {

    public static HikariDataSource newHikariDataSource(String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setDriverClassName(Constans.PHOENIX_DRIVER);
        if (!Strings.isNullOrEmpty(username)) config.setUsername(username);
        if (!Strings.isNullOrEmpty(password)) config.setPassword(password);
        config.setIdleTimeout(60000);
        config.setValidationTimeout(3000);
        config.setConnectionTestQuery("select 1");
        config.setMaxLifetime(60000);
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30000); // 设置连接超时时间
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setAutoCommit(true); // phoenix 驱动不会自动 commit，需要加入默认的auto commit设置，否则更新操作的数据会缓存在本地直到缓冲区满

        return new HikariDataSource(config);
    }

}
