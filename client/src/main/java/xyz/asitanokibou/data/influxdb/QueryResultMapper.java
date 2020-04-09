package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.QueryResult;
/**
 * @author aimysaber@gmail.com
 */
public interface QueryResultMapper<T> {

    T call(QueryResult queryResult);
}
