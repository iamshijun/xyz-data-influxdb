package xyz.asitanokibou.data.influxdb.ex;

/**
 * @author aimysaber@gmail.com
 */
public class InfluxDBXException extends RuntimeException {

    public InfluxDBXException(String message, Throwable cause) {
        super(message, cause);
    }

    public InfluxDBXException(String message) {
        super(message);
    }
}
