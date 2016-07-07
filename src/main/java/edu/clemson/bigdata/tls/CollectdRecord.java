package edu.clemson.bigdata.tls;

import org.apache.flink.streaming.connectors.json.JSONParser;
import org.apache.sling.commons.json.JSONException;

/**
 * A CollectdRecord is a record received from collectd.
 */
public class CollectdRecord {

  private long values;
  private String dstypes;
  private String dsnames;
  private Long time;
  private double interval;
  private String host;
  private String plugin;
  private String plugin_instance;
  private String type;
  private String type_instance;

  public CollectdRecord() {
  }

  public CollectdRecord(long values, String dstypes, String dsnames, Long time,
                        double interval, String host, String plugin, String plugin_instance,
                        String type, String type_instance) {
    this.values = values;
    this.dstypes = dstypes;
    this.dsnames = dsnames;
    this.time = time;
    this.interval = interval;
    this.host = host;
    this.plugin = plugin;
    this.plugin_instance = plugin_instance;
    this.type = type;
    this.type_instance = type_instance;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(values).append(",");
    sb.append(dstypes).append(",");
    sb.append(dsnames).append(",");
    sb.append(time).append(",");
    sb.append(interval).append(",");
    sb.append(host).append(",");
    sb.append(plugin).append(",");
    sb.append(plugin_instance).append(",");
    sb.append(type).append(",");
    sb.append(type_instance).append(",");

    return sb.toString();
  }

  public static CollectdRecord fromJson(String message) {

    CollectdRecord record = new CollectdRecord();

    try {
      // Retrieve json body by trimming head and tail
      JSONParser parser = new JSONParser(message.substring(1, message.length()));

      record.values = parser.parse("values").getJSONArray("retValue").getLong(0);
      record.dstypes = parser.parse("dstypes").getJSONArray("retValue").getString(0);
      record.dsnames = parser.parse("dsnames").getJSONArray("retValue").getString(0);
      record.time = (long) parser.parse("time").getDouble("retValue") * 1000;
      record.interval = parser.parse("interval").getDouble("retValue");
      record.host = parser.parse("host").getString("retValue");
      record.plugin = parser.parse("plugin").getString("retValue");
      record.plugin_instance = parser.parse("plugin_instance").getString("retValue");
      record.type = parser.parse("type").getString("retValue");
      record.type_instance = parser.parse("type_instance").getString("retValue");
    } catch (JSONException e) {
      throw new RuntimeException("Invalid record:" + message, e);
    }

    return record;
  }

  public void setValues(long values) {
    this.values = values;

  }

  public void setDstypes(String dstypes) {
    this.dstypes = dstypes;
  }

  public void setDsnames(String dsnames) {
    this.dsnames = dsnames;
  }

  public void setTime(Long time) {
    this.time = time;
  }

  public void setInterval(float interval) {
    this.interval = interval;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPlugin(String plugin) {
    this.plugin = plugin;
  }

  public void setPlugin_instance(String plugin_instance) {
    this.plugin_instance = plugin_instance;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setType_instance(String type_instance) {
    this.type_instance = type_instance;
  }

  public long getValues() {

    return values;
  }

  public String getDstypes() {
    return dstypes;
  }

  public String getDsnames() {
    return dsnames;
  }

  public Long getTime() {
    return time;
  }

  public double getInterval() {
    return interval;
  }

  public String getHost() {
    return host;
  }

  public String getPlugin() {
    return plugin;
  }

  public String getPlugin_instance() {
    return plugin_instance;
  }

  public String getType() {
    return type;
  }

  public String getType_instance() {
    return type_instance;
  }
}
