package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.QueryResult;

import java.util.Collection;
/**
 * @author aimysaber@gmail.com
 */
public interface QueryResultCallback {

    void call(Collection<QueryResult.Result> queryResults);
}
