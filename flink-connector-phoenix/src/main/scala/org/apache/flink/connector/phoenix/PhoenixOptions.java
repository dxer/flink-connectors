package org.apache.flink.connector.phoenix;

import java.io.Serializable;

public class PhoenixOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private String serverUrl;

    private String tableName;

    public PhoenixOptions(String serverUrl, String tableName) {
        this.serverUrl = serverUrl;
        this.tableName = tableName;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "PhoenixOptions{" +
                "serverUrl='" + serverUrl + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
