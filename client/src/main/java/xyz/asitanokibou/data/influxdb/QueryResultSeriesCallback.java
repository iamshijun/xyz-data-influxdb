package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.QueryResult;
/**
 * @author aimysaber@gmail.com
 */
public interface QueryResultSeriesCallback {

    void call(QueryResult.Series series);
}
