package xyz.asitanokibou.data.influxdb;

import okhttp3.Interceptor;

/**
 * @author aimysaber@gmail.com
 */
public class InfluxDBClientConfig {

    private String dburl;
    private String username;
    private String password;

    private int connectionTimeout = 3000;
    private int soTimeout = 20_000;

    private boolean httpLog = false;

//    List<Interceptor> applicationInterceptors;
//    List<Interceptor> networkInterceptors;

}
