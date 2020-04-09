package xyz.asitanokibou.data.influxdb.ex;

/**
 * @author aimysaber@gmail.com
 */
public class InfluxDBXConnectionException extends RuntimeException {

    public InfluxDBXConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InfluxDBXConnectionException(String message) {
        super(message);
    }


}
