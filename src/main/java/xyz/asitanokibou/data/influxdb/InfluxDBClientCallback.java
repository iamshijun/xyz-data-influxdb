package xyz.asitanokibou.data.influxdb;

import org.influxdb.InfluxDB;

/**
 * @author aimysaber@gmail.com
 */
public interface InfluxDBClientCallback<T> {
    T execute(InfluxDB client);
}
