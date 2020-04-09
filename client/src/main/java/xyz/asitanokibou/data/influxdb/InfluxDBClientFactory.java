package xyz.asitanokibou.data.influxdb;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BoundParameterQuery;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.*;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author aimysaber@gmail.com
 */
@Getter
@Setter
@Slf4j(topic = "influxdb.query")
public class InfluxDBClientFactory implements PooledObjectFactory<InfluxDB> {

    private final String dburl;
    //InfluxDBOkHttpConfig
    private int connectionTimeout = 3000;
    private int soTimeout = 20_000;
    private String username;
    private String password;
    private String defaultDatabase;

    private boolean logHttp;
    private boolean logQueryResult;
    // private String clientName;
    private InfluxDBClientPool innerPool;

    public InfluxDBClientFactory(String dburl) {
        this(dburl, null, null);
    }

   /* public InfluxDBClientFactory(InfluxDBClientConfig influxDBClientConfig) {
        this.dburl = influxDBClientConfig.
    }*/

    public InfluxDBClientFactory(String dburl, String username, String password) {
        this.dburl = dburl;
        this.username = username;
        this.password = password;
    }

    void setInnerPool(InfluxDBClientPool innerPool) {
        this.innerPool = innerPool;
    }

    @Override
    public PooledObject<InfluxDB> makeObject() {

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(soTimeout, TimeUnit.MILLISECONDS)
                //.socketFactory()
                //.sslSocketFactory()
                ;

        if (logHttp) {
            HttpLoggingInterceptor httpLoggingInterceptor = new HttpLoggingInterceptor();
            httpLoggingInterceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
            builder.addNetworkInterceptor(httpLoggingInterceptor);
        }

        InfluxDB client;
        if (Utils.isEmpty(username) || Utils.isEmpty(password)) {
            client = InfluxDBFactory.connect(dburl, builder);
        } else {
            client = InfluxDBFactory.connect(dburl, username, password, builder);
        }
        if (Utils.isNotEmpty(defaultDatabase)) {
            client.setDatabase(defaultDatabase);
        }

        //client config/settings - 看下是全局设置还是在使用的时候设置然后return的时候重置?
//        client.disableBatch();
//        client.disableGzip();

        return new DefaultPooledObject<>(InfluxDBProxy.newProxy(client, innerPool, logQueryResult));
    }

    @Override
    public void destroyObject(PooledObject<InfluxDB> pooledObject) {
        InfluxDB client = getClient(pooledObject);
        if (client != null) {

            if (client instanceof InfluxDBproxyHelper) {
//                ((InfluxDBproxyHelper) client).getTarget().close();
                ((InfluxDBproxyHelper) client).close();
            } else {
                client.close();
            }
        }
    }

    private InfluxDB getClient(PooledObject<InfluxDB> pooledObject) {
        return pooledObject.getObject();
    }

    @Override
    public boolean validateObject(PooledObject<InfluxDB> pooledObject) {
        InfluxDB client = getClient(pooledObject);
        try {
            client.ping();
            return true;
        } catch (InfluxDBIOException io) {
            return false;
        }
    }

    @Override
    public void activateObject(PooledObject<InfluxDB> p) {
        //dummy
    }

    @Override
    public void passivateObject(PooledObject<InfluxDB> p) {
        //dummy
    }


    //package visible
    interface InfluxDBproxyHelper extends Closeable {
        InfluxDB getTarget();

        InfluxDBClientPool getDataSource();

        @Override
        default void close() {
            getTarget().close();
        }
    }

    @Getter
    private static class InfluxDBProxy implements InvocationHandler {

        private final InfluxDB target;
        private final InfluxDBClientPool datasource;

        private boolean isBroken;

        @Setter
        private boolean logQueryResult;

        private InfluxDBProxy(InfluxDB target, InfluxDBClientPool datasource, boolean logQueryResult) {
            this.target = target;
            this.datasource = datasource;
            this.logQueryResult = logQueryResult;
        }

        static InfluxDB newProxy(InfluxDB client, InfluxDBClientPool datasource) {
            return newProxy(client, datasource, false);
        }

        static InfluxDB newProxy(InfluxDB client, InfluxDBClientPool datasource, boolean logQueryResult) {
            if (datasource == null) {//如果没有datasource就暂时不代理 - 代理只处理close的时候将target放回到对象池中
                return client;
            }
            Objects.requireNonNull(client);
            return (InfluxDB) Proxy.newProxyInstance(Utils.getClassLoader(),
                    new Class[]{InfluxDB.class, InfluxDBproxyHelper.class},
                    new InfluxDBProxy(client, datasource, logQueryResult));
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            Class<?> declaringClass = method.getDeclaringClass();

            String methodName = method.getName();
            Class<?> returnType = method.getReturnType();

            if (declaringClass == InfluxDBproxyHelper.class) {
                if (methodName.equals("getTarget")) {
                    return this.target;
                } else if (methodName.equals("getDataSource")) {
                    return this.datasource;
                }
                return null;

            } else {
                if (methodName.equals("close") && datasource != null) {
                    if (isBroken) {
                        datasource.invalidateObject((InfluxDB) proxy);
                    } else {
                        datasource.returnObject((InfluxDB) proxy);
                    }
                    return null;
                } else if (methodName.equals("query") && (args.length > 1 && args[0] instanceof Query)/*in case..*/) {
                    Query query = (Query) args[0];
                    logQuery(query);
                }

                try {
                    Object result = method.invoke(target, args);

                    if (methodName.equals("query") && logQueryResult && returnType == QueryResult.class) {
                        log.debug("Query Result : {}", result);
                    }
                    return result;
                } catch (InvocationTargetException e) {
                    if (e.getCause() instanceof IOException) {//TODO 需要看具体是什么异常
                        //add log...
                        isBroken = true;
                    }
                    throw e;
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void logQuery(Query query) {
            if (log.isDebugEnabled()) {
                String queryString = query.getCommand();

                if (query instanceof BoundParameterQuery) {
                    Field paramsField = Utils.findField(BoundParameterQuery.class, "params");

                    if (paramsField != null) {
                        Utils.makeAccessible(paramsField);
                        Map<String, Object> params = (Map<String, Object>) Utils.getField(paramsField, query);

                        if (params != null) {

                            for (Map.Entry<String, Object> enrty : params.entrySet()) {
                                String paramName = enrty.getKey();
                                String paramValue = String.valueOf(enrty.getValue());

                                queryString = queryString.replaceAll("\\$" + paramName, "'" + paramValue + "'");
                            }
                        }
                    }
                    log.debug("Query Command : {}", queryString);
                }
            }
        }

    }
}
