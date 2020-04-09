package xyz.asitanokibou.data.influxdb;

import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.influxdb.InfluxDB;

/**
 * @author aimysaber@gmail.com
 */
//TODO 使用组合方式不用继承(InfluxDBPool)  当前发现作为bean被spring管理的时候发现重复注册MBean的情况
public class InfluxDBClientPool extends GenericObjectPool<InfluxDB> {

    public static InfluxDBClientPool create(String url,String username,String password){
        return new InfluxDBClientPool(url, username, password);
    }

    private InfluxDBClientPool(String url,String username,String password){
        this(new InfluxDBClientFactory(url, username, password));
    }

    public InfluxDBClientPool(InfluxDBClientFactory factory) {
        super(factory);
        factory.setInnerPool(this);
    }

    public InfluxDBClientPool(InfluxDBClientFactory factory, GenericObjectPoolConfig<InfluxDB> config) {
        super(factory, config);
        factory.setInnerPool(this);
    }

    public InfluxDBClientPool(InfluxDBClientFactory factory, GenericObjectPoolConfig<InfluxDB> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
        factory.setInnerPool(this);
    }
}
