package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.QueryResult;

import java.util.List;
/**
 * @author aimysaber@gmail.com
 */
public interface InfluxDBXMapper<T> {

    List<T> mapResult(QueryResult result);
}
