package com.yahoo.ycsb.db;

import com.sun.net.ssl.internal.www.protocol.https.HttpsURLConnectionOldImpl;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.text.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Prometheus client for YCSB framework.
 */
public class PrometheusClient extends DB {
    private final int SUCCESS = 0;
    private URL urlQuery = null;
    private URL urlPut = null;
    private String ip_pushgateway = "localhost";
    private String ip_prometheus = "localhost";
    private String putURL = "/api/put";
    private int port_pushgateway = 9091;
    private int port_prometheus = 9090;
    private boolean _debug = false;
    private boolean useCount = true;
    private boolean usePlainTextFormat = true;
    private CloseableHttpClient client;
    private int retries = 3;
    private boolean test = false;

    private static final DateFormat rfc3339Format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final String metricRegEx = "[a-zA-Z_:][a-zA-Z0-9_:]*";
    private String queryURLInfix = "";

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        try {
            test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));

            if (!getProperties().containsKey("ip_prometheus") && !test) {
                throw new DBException("No ip_server given, abort.");
            }
            ip_prometheus = getProperties().getProperty("ip_prometheus", ip_prometheus);

            if (!getProperties().containsKey("port_prometheus") && !test) {
                throw new DBException("No port_prometheus given, abort.");
            }
            port_prometheus = Integer.parseInt(getProperties().getProperty("port_prometheus", String.valueOf(port_prometheus)));

            if (!getProperties().containsKey("ip_pushgateway") && !test) {
                throw new DBException("No ip_pushgateway given, abort.");
            }
            ip_pushgateway = getProperties().getProperty("ip_pushgateway", ip_pushgateway);
            if (!getProperties().containsKey("port_pushgateway") && !test) {
                throw new DBException("No port_pushgateway given, abort.");
            }
            port_pushgateway = Integer.parseInt(getProperties().getProperty("port_pushgateway", String.valueOf(port_pushgateway)));
			
			if (getProperties().containsKey("queryURLInfix")) {
                queryURLInfix = getProperties().getProperty("queryURLInfix", queryURLInfix);
            }            

            if (_debug) {
                System.out.println("The following properties are given: ");
                for (String element : getProperties().stringPropertyNames()) {
                    System.out.println(element + ": " + getProperties().getProperty(element));
                }
            }
            usePlainTextFormat = Boolean.parseBoolean(getProperties().getProperty("plainTextFormat", "true"));
            useCount = Boolean.parseBoolean(getProperties().getProperty("useCount", "true"));
            RequestConfig requestConfig = RequestConfig.custom().build();
            if (!test) {
                client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
            }
        } catch (Exception e) {
            throw new DBException(e);
        }

        try {
            urlQuery = new URL("http", ip_prometheus, port_prometheus, queryURLInfix);
            if (_debug) {
                System.out.println("URL: " + urlQuery);
            }
            urlPut = new URL("http", ip_pushgateway, port_pushgateway, putURL);
            if (_debug) {
                System.out.println("URL: " + urlPut);
            }
        } catch (MalformedURLException e) {
            throw new DBException(e);
        }
    }

    private JSONArray runQuery(URL url, String queryStr) {
        JSONArray jsonArr = new JSONArray();
        HttpResponse response = null;
        try {
            HttpPut postMethod = new HttpPut(url.toURI());

            StringEntity requestEntity = new StringEntity(
                    queryStr + "\n");
            requestEntity.setContentType("text/html; charset=UTF-8\\n; version=0.0.4;");
            postMethod.addHeader("host", ip_pushgateway);
            postMethod.addHeader("Accept", "application/json");
            postMethod.setEntity(requestEntity);

            int tries = retries + 1;
            while (true) {
                tries--;
                try {
                    response = client.execute(postMethod);
                    String inputLine = "";
                    BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                    try {
                        while ((inputLine = br.readLine()) != null) {
                            System.out.println("1" + inputLine);
                        }
                        br.close();
                    } catch (IOException e) {
                        //e.printStackTrace();
                    }
                    break;
                } catch (IOException e) {
                    if (tries < 1) {
                        System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
                        e.printStackTrace();
                        if (response != null) {
                            EntityUtils.consumeQuietly(response.getEntity());
                        }
                        postMethod.releaseConnection();
                        return null;
                    }
                }
            }
            if (response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300) {
                if (response.getStatusLine().getStatusCode() == HttpsURLConnectionOldImpl.HTTP_ACCEPTED) {
                    // The pushgateway does not include an entity in the response when inserting
                    if (response.getEntity().getContent().available() == 0) {
                        jsonArr = new JSONArray();
                        return jsonArr;
                    }
                }
                if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
                    BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                    StringBuilder builder = new StringBuilder();
                    String line;
                    while ((line = bis.readLine()) != null) {
                        builder.append(line);
                    }
                    jsonArr = new JSONArray(builder.toString());
                }
                EntityUtils.consumeQuietly(response.getEntity());
                postMethod.releaseConnection();
            }

        } catch (Exception e) {
            System.err.println("ERROR: Error while trying to query " + url.toString() + " for '" + queryStr + "'.");
            e.printStackTrace();
            if (response != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
            return null;
        }
        return jsonArr;
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        try {
            if (!test) {
                client.close();
            }
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Read a record from the database. Each value from the result will be stored in a HashMap
     *
     * @param metric    The name of the metric
     * @param timestamp The timestamp of the record to read.
     * @param tags      actual tags that were want to receive (can be empty)
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
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
                queryString += values.get(i)
                        + (i + 1 < (values.size()) ? "|" : "");
            }
            queryString += "\",";
        }
        queryString = "{" + (queryString.isEmpty() ? "" : queryString.substring(0, queryString.length() - 1)) + "}";

        try {
            queryString = URLEncoder.encode(queryString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return -1;
        }

        queryString = "?query=" + metric + queryString;
        queryString += "&time=" + rfc3339Format.format(new Date(timestamp.getTime())).replace("+", "%2B");

        if (_debug)
            System.out.println("Input Query: " + urlQuery.toString() + queryString);
        getMethod = new HttpGet(urlQuery.toString() + queryString);
        loop:
        while (true) {
            tries--;
            try {
                if (test)
                    return SUCCESS;

                response = client.execute(getMethod);

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
                    return SUCCESS;
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
     * @param metric    The name of the metric
     * @param startTs   The timestamp of the first record to read.
     * @param endTs     The timestamp of the last record to read.
     * @param tags      actual tags that were want to receive (can be empty)
     * @param avg       do averaging
     * @param sum       do summarize
     * @param count     do count
     * @param timeValue value for timeUnit for sum/count/avg
     * @param timeUnit  timeUnit for sum/count/avg
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,
            ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit) {

        if (metric == null || metric.isEmpty() || !metric.matches(metricRegEx)) {
            return -1;
        }
        if (startTs == null || endTs == null) {
            return -1;
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
            queryString = URLEncoder.encode("(" + metric + queryString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return -1;
        }
        // Duration are converted to seconds anyway, so always use those
        // No application of functions on buckets possible, timeValue is ignored

        if (avg) {
            queryString = "?query=avg_over_time" + queryString;

        } else if (count) {
            if (useCount) {
                queryString = "?query=count_over_time" + queryString;

            } else {
                queryString = "?query=min_over_time" + queryString;

            }
        } else if (sum) {
            queryString = "?query=sum_over_time" + queryString;

        } else {
            queryString = "?query=min_over_time" + queryString;


        }
        if (_debug)
            System.out.println("Input Query: " + urlQuery.toString() + queryString);
        getMethod = new HttpGet(urlQuery.toString() + queryString);
        loop:
        while (true) {
            tries--;
            try {
                if (test)
                    return SUCCESS;

                response = client.execute(getMethod);

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
                        if (responseData.getJSONObject("data").getJSONArray("result").length() > 0)
                            return SUCCESS;
                        else
                            return -1;
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
        if (metric == null || metric.isEmpty() || !metric.matches(metricRegEx) || timestamp == null) {
            return -1;
        }
        if (usePlainTextFormat) {
            String queryString = "#TYPE " + metric + " gauge\n" + metric;
            if (tags.size() > 0) {
                queryString += "{";
                for (String tagKey : tags.keySet())
                    queryString += tagKey + "=\"" +
                            (tags.get(tagKey).toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")) +
                            "\",";
                queryString = queryString.substring(0, queryString.length() - 1) + "} " + value + " " + timestamp.getTime();
            } else
                queryString += " " + value + " " + timestamp.getTime();

            try {

                if (_debug) {
                    System.out.println("Timestamp: " + timestamp.getTime());
                    System.out.println("Input Query String: " + queryString);
                }
                if (test) {
                    return SUCCESS;
                }
                JSONArray jsonArr = runQuery(urlPut, queryString);
                if (_debug) {
                    System.err.println("jsonArr: " + jsonArr);
                }
                if (jsonArr == null) {
                    System.err.println("ERROR: Error in processing insert to metric: " + metric);
                    return -1;
                }
                return SUCCESS;

            } catch (Exception e) {
                System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
                e.printStackTrace();
                return -1;
            }
        } else {
            // No usage of custom timestamps possible
            CollectorRegistry registry = new CollectorRegistry();
            Gauge gauge = Gauge.build().name(metric).help(timestamp.toString()).labelNames(tags.keySet().toArray(new String[]{})).create();
            Gauge.Child child = gauge.labels(tags.keySet().toArray(new String[]{}));
            child.set(value);
            gauge.register(registry);

            PushGateway gateway = new PushGateway(ip_pushgateway + ":" + port_pushgateway);
            int tries = retries;
            loop:
            while (true) {
                try {
                    tries--;
                    gateway.pushAdd(registry, "test_job");
                    return 1;
                } catch (IOException exception) {
                    if (tries > 0)
                        continue loop;
                    System.err.println(exception.toString());
                    return -1;
                }
            }
        }
    }

}

