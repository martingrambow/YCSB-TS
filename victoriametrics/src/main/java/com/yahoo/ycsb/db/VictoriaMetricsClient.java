package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * VictoriaMetrics client for YCSB framework.
 * Mix of influxDB client (Inserts) and Prometheus client (Scan & Read)
 */
public class VictoriaMetricsClient extends DB {

    private String ip = "localhost";
    private String dbName = "testdb";
    private int port = 8086;
    private String queryURLInfix = "/api/v1/query";
    private boolean _debug = true;
    private String valueFieldName = "value"; // in which field should the value be?

    private InfluxDB influxClient;
    private CloseableHttpClient victoriaClient;
    private int retries = 3;
    private URL urlQuery = null;

    private final int SUCCESS = 0;
    private static final String metricRegEx = "[a-zA-Z_:][a-zA-Z0-9_:]*";
    private static final DateFormat rfc3339Format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        try {
            port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
            dbName = getProperties().getProperty("dbName", dbName);
            ip = getProperties().getProperty("ip", ip);
            if (_debug) {
                System.out.println("The following properties are given: ");
                for (String element : getProperties().stringPropertyNames()) {
                    System.out.println(element + ": " + getProperties().getProperty(element));
                }
            }
            this.influxClient = InfluxDBFactory.connect(String.format("http://%s:%s", ip, port), "root", "root");
            if (_debug) {
                this.influxClient = this.influxClient.setLogLevel(InfluxDB.LogLevel.FULL);
            }
            urlQuery = new URL("http", ip, port, queryURLInfix);
            RequestConfig requestConfig = RequestConfig.custom().build();
            victoriaClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        }
        catch (retrofit.RetrofitError e) {
            throw new DBException(String.format("Can't connect to %s:%s.)", ip, port)+e);
        }
        catch (Exception e) {
            throw new DBException(e);
        }
        if (_debug) {
            System.out.println("Initialized.");
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (_debug) {
            System.out.println("Cleanup...");
        }
    }

    /**
     * Read a record from the database.
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
        if (_debug)
            System.out.println("Read...");
        if (metric == null || metric.isEmpty() || !metric.matches(metricRegEx)) {
            return -1;
        }
        if (timestamp == null) {
            return -1;
        }
        int tries = retries + 1;
        HttpGet getMethod;
        String queryString = "";
        HttpResponse response = null;
        JSONObject responseData;

        // Construct query
        for (Map.Entry entry : tags.entrySet()) {
            queryString += entry.getKey() + "=~\"";
            ArrayList<String> values = (ArrayList<String>) entry.getValue();
            for (int i = 0; i < values.size(); i++) {
                queryString += values.get(i) + (i + 1 < (values.size()) ? "|" : "");
            }
            queryString += "\",";
        }
        if (_debug)
            System.out.println("Query with tags: " + queryString);
        queryString = "{" + (queryString.isEmpty() ? "" : queryString.substring(0, queryString.length() - 1)) + "}";

        try {
            queryString = URLEncoder.encode(queryString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return -1;
        }

        queryString = "?query=" + metric + "_" + valueFieldName + queryString;
        queryString += "&time=" + rfc3339Format.format(new Date(timestamp.getTime())).replace("+", "%2B");

        if (_debug)
            System.out.println("Read Query: " + urlQuery.toString() + queryString);
        getMethod = new HttpGet(urlQuery.toString() + queryString);
        loop:
        while (true) {
            tries--;
            try {
                response = victoriaClient.execute(getMethod);
                String inputLine = "";
                String content = "";
                BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                try {
                    while ((inputLine = br.readLine()) != null) {
                        content += inputLine;
                    }
                    br.close();
                } catch (IOException e) {
                }
                responseData = new JSONObject(content);
                if (responseData.getString("status").equals("success")) {
                    if (responseData.getJSONObject("data").getJSONArray("result").length() > 0) {
                        if (_debug)
                            System.out.println("Found " + responseData.getJSONObject("data").getJSONArray("result").length() + " data sets");
                        return SUCCESS;
                    }
                    else
                    if (_debug)
                        System.out.println("No data in response");
                    return -1;
                }
            } catch (IOException e) {
                if (tries < 1) {
                    System.err.print("ERROR: Connection to " + urlQuery.toString() + " failed " + retries + "times.");
                    e.printStackTrace();
                    if (response != null) {
                        EntityUtils.consumeQuietly(response.getEntity());
                    }
                    EntityUtils.consumeQuietly(response.getEntity());
                    getMethod.releaseConnection();
                    return -1;
                }
                continue loop;
            }
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each value from the result will be stored in a HashMap.
     *
     * @param metric  The name of the metric
     * @param startTs The timestamp of the first record to read.
     * @param endTs   The timestamp of the last record to read.
     * @param tags     actual tags that were want to receive (can be empty)
     * @param avg    do averageing
     * @param sum    do summarize
     * @param count  do count
     * @param timeValue  value for timeUnit for sum/count/avg
     * @param timeUnit  timeUnit for sum/count/avg
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit ) {
        if (_debug)
            System.out.println("Scan...");
        if (metric == null || metric.isEmpty() || !metric.matches(metricRegEx)) {
            return -1;
        }
        if (startTs == null || endTs == null) {
            return -1;
        }

        if (tags.size() > 0) {
            System.out.println("THERE ARE TAGS!!!!!");
            System.out.println("###################################");
        }

        NumberFormat durationOffsetFormat = new DecimalFormat("###");
        int tries = retries + 1;
        HttpGet getMethod;
        String queryString = "";
        HttpResponse response = null;
        JSONObject responseData;
        double duration;
        double offset;
        double currentTime = new Date().getTime();

        for (Map.Entry entry : tags.entrySet()) {
            queryString += entry.getKey() + "=~\"";
            ArrayList<String> values = (ArrayList<String>) entry.getValue();
            for (int i = 0; i < values.size(); i++) {
                queryString += values.get(i)
                        + (i + 1 < (values.size()) ? "|" : "");
            }
            queryString += "\",";
        }
        if (tags.entrySet().size() == 0) {
            queryString = ",";
        }
        if (_debug)
            System.out.println("Query with tags: " + urlQuery.toString() + queryString);

         /* Application of aggregations by bucket not possible, timeValue and timeUnit ignored
         query_range would not be suitable, as only 11.000 entries are possible
         and those are made up interpolated values and those cannot be aggregated because of the response format */
        duration = Math.ceil(((double) endTs.getTime() - startTs.getTime()) / 1000d);
        offset = (long) Math.floor((currentTime - endTs.getTime()) / 1000d);
        if ((currentTime - offset - duration) > (startTs.getTime() / 1000d))
            duration++;

        queryString = "{" + queryString.substring(0, queryString.length() - 1) + "}[" +
                durationOffsetFormat.format(duration) + "s]offset " + durationOffsetFormat.format(offset) + "s)";
        try {
            queryString = URLEncoder.encode("(" + metric + "_" + valueFieldName + queryString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return -1;
        }
        // Duration are converted to seconds anyway, so always use those
        // No application of functions on buckets possible, timeValue is ignored
        if (avg) {
            queryString = "?query=avg_over_time" + queryString;
        } else if (count) {
            queryString = "?query=count_over_time" + queryString;
        } else if (sum) {
            queryString = "?query=sum_over_time" + queryString;
        } else {
            queryString = "?query=min_over_time" + queryString;
        }
        if (_debug)
            System.out.println("Query: " + urlQuery.toString() + queryString);
        getMethod = new HttpGet(urlQuery.toString() + queryString);
        loop:
        while (true) {
            tries--;
            System.out.println("Tries: " + tries);
            try {
                response = victoriaClient.execute(getMethod);
                String inputLine = "";
                String content = "";
                BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                try {
                    while ((inputLine = br.readLine()) != null) {
                        content += inputLine;
                    }
                    br.close();
                } catch (IOException e) {
                }
                responseData = new JSONObject(content);
                if (responseData.getString("status").equals("success")) {
                    try {
                        if (responseData.getJSONObject("data").getJSONArray("result").length() > 0) {
                            if (_debug) {
                                System.out.println("Found " + responseData.getJSONObject("data").getJSONArray("result").length() + " data sets");
                                System.out.println("SCAN done.");
                            }
                            return SUCCESS;
                        }
                        else {
                            if (_debug) {
                                System.out.println("No data in response");
                                System.out.println("SCAN done.");
                            }
                            return -1;
                        }
                    } catch (JSONException e) {
                        // No data included in response
                        EntityUtils.consumeQuietly(response.getEntity());
                        getMethod.releaseConnection();
                        return -1;
                    }
                }
            } catch (IOException e) {
                if (tries < 1) {
                    System.err.print("ERROR: Connection to " + urlQuery.toString() + " failed " + retries + "times.");
                    e.printStackTrace();
                    if (response != null) {
                        EntityUtils.consumeQuietly(response.getEntity());
                    }
                    EntityUtils.consumeQuietly(response.getEntity());
                    getMethod.releaseConnection();
                    return -1;
                }
                continue loop;
            }
        }
    }

    /**
     * Insert a record in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
     * will be written into the record with the specified timestamp
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to insert.
     * @param value     actual value to insert
     * @param tags      A HashMap of tag/tagvalue pairs to insert as tags
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
        if (_debug) {
            System.out.println("Insert...");
        }
        if (metric == null || metric == "") {
            return -1;
        }
        if (timestamp == null) {
            return -1;
        }

        try {
            Point.Builder pb = Point.measurement(metric)
                    .time(timestamp.getTime(), TimeUnit.MILLISECONDS);
            for ( Map.Entry entry : tags.entrySet()) {
                pb = pb.tag(entry.getKey().toString(), entry.getValue().toString());
            }
            pb = pb.field(this.valueFieldName, String.valueOf(value));
            // "default" = retentionPolicy
            this.influxClient.write(this.dbName, "default", pb.build());
            return SUCCESS;
        } catch (Exception e) {
            System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
            e.printStackTrace();
            return -1;
        }
    }
}