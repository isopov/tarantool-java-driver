package com.sopovs.moradanen.tarantool.jdbc;

import com.sopovs.moradanen.tarantool.TarantoolClientImpl;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class TarantoolDriver implements Driver {

    private static final String JDBC_TARANTOOL = "jdbc:tarantool://";

    static {
        try {
            DriverManager.registerDriver(new TarantoolDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public TarantoolConnection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Properties props = parseProperties(url, info);
        return new TarantoolConnection(
                new TarantoolClientImpl(props.getProperty("host"), Integer.valueOf(props.getProperty("port", "3301")),
                        props.getProperty("user"), props.getProperty("password")));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties props = parseProperties(url, info);
        DriverPropertyInfo host = new DriverPropertyInfo("host", props.getProperty("host"));
        host.required = true;
        DriverPropertyInfo port = new DriverPropertyInfo("port", props.getProperty("port", "3301"));
        port.required = true;
        return new DriverPropertyInfo[]{host, port, new DriverPropertyInfo("user", props.getProperty("user")),
                new DriverPropertyInfo("password", props.getProperty("password"))};
    }

    private Properties parseProperties(String url, Properties info) throws SQLException {
        Properties result = new Properties(info);
        if (acceptsURL(url)) {
            String urlValue = url.substring(JDBC_TARANTOOL.length());
            if (urlValue.contains("?")) {
                parseHostPort(urlValue.substring(0, urlValue.indexOf('?')), result);
                parseParameters(urlValue.substring(urlValue.indexOf('?') + 1), result);
            } else {
                parseHostPort(urlValue, result);
            }
        }
        return result;
    }

    private static void parseParameters(String parameters, Properties props) throws SQLException {
        for (String paramPair : parameters.split("&")) {
            if (!paramPair.contains("=")) {
                throw new SQLException("Cannot parse url parameter " + paramPair + " param=value is expected");
            }

            int indexOfEquals = paramPair.indexOf('=');
            props.put(paramPair.substring(0, indexOfEquals), paramPair.substring(indexOfEquals + 1));
        }
    }

    private static void parseHostPort(String urlPart, Properties props) {
        if (urlPart.contains(":")) {
            int indexOfColon = urlPart.indexOf(':');
            props.put("host", urlPart.substring(0, indexOfColon));
            props.put("port", urlPart.substring(indexOfColon + 1));
        } else {
            props.put("host", urlPart);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(JDBC_TARANTOOL);
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

}
