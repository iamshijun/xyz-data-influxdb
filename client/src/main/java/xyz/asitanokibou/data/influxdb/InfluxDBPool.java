package xyz.asitanokibou.data.influxdb;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.influxdb.InfluxDB;
/**
 * @author aimysaber@gmail.com
 */
public class InfluxDBPool {

    private GenericObjectPool<InfluxDB> pool;

    public void setPool(GenericObjectPool<InfluxDB> pool) {
        this.pool = pool;
    }
}
