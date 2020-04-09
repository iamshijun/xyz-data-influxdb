package xyz.asitanokibou.data.influxdb;

import org.influxdb.InfluxDB;

public abstract class InfluxDBClientCallbackWithNoResult implements InfluxDBClientCallback<Object> {

    @Override
    public Object execute(InfluxDB client) {
        executeWithNoResult(client);
        return null;
    }

    public abstract void executeWithNoResult(InfluxDB client);
}
