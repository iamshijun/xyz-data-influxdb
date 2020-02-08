package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.QueryResult;
/**
 * @author aimysaber@gmail.com
 */
public interface QueryResultSeriesMapper<T> {

    T call(QueryResult.Series series);
}
