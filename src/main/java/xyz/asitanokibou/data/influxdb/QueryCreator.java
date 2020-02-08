package xyz.asitanokibou.data.influxdb;

import org.influxdb.dto.Query;
/**
 * @author aimysaber@gmail.com
 */
public interface QueryCreator {

    Query createQuery();
}
