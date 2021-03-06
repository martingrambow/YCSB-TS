package com.yahoo.ycsb.db;

import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * Akumuli client for YCSB-TS framework.<br>
 * Restrictions:<br>
 * Timestamps are stored in nanosecond precision. Functions count and sum are
 * not supported - for those max will be used instead.
 *
 * @author Rene Trefft
 */
public class AkumuliClient extends DB {

	private boolean test = false;
	private final boolean _debug = false;

	private final int SUCCESS = 0;

	/** Inserting with TCP Server */
	private Socket insertSocket;
	private PrintWriter insertWriter;

	private HttpClient akumuliHTTPClient;

	/** URL to HTTP API */
	private String akumuliHTTPUrl;

    private TimeZone tz;
    private DateFormat df;

	@Override
	public void init() throws DBException {

		test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));

		if (!getProperties().containsKey("ip") && !test) {
			throw new DBException("No ip given, abort.");
		}

		if (!getProperties().containsKey("tcpPort") && !test) {
			throw new DBException("No TCP Server port given, abort.");
		}

		if (!getProperties().containsKey("httpPort") && !test) {
			throw new DBException("No HTTP Server port given, abort.");
		}

		String ip = getProperties().getProperty("ip", "localhost");
		int tcpPort = Integer.parseInt(getProperties().getProperty("tcpPort", "8282"));
		int httpPort = Integer.parseInt(getProperties().getProperty("httpPort", "8181"));
        tz = TimeZone.getTimeZone("UTC");
        df = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSSSSSS");
        df.setTimeZone(tz);

//		String ip = "192.168.209.244";
//		int tcpPort = 8282;
//		int httpPort = 8181;

		if (_debug) {
			System.out.println("The following properties are given: ");
			for (String element : getProperties().stringPropertyNames()) {
				System.out.println(element + ": " + getProperties().getProperty(element));
			}
		}

		if (!test) {

			try {

				insertSocket = new Socket(ip, tcpPort);
				insertWriter = new PrintWriter(insertSocket.getOutputStream(), false);

				akumuliHTTPUrl = "http://" + ip + ':' + httpPort;
				akumuliHTTPClient = HttpClients.createDefault();

			} catch (Exception e) {
				throw new DBException(e);
			}

		}

	}

	@Override
	public void cleanup() throws DBException {
		try {
			insertWriter.close();
			insertSocket.close();
		} catch (Exception e) {
			throw new DBException(e);
		}
	}

	private long getNanoSecOfTimestamp(Timestamp timestamp) {
		return timestamp.getTime() / 1000 * 1000000000 + timestamp.getNanos();
	}

	@Override
	public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {

		if (metric == null || metric.isEmpty() || timestamp == null) {
			return -1;
		}

		String timestampNanos = df.format(timestamp)+timestamp.getNanos();
		JSONObject range = new JSONObject().put("from", timestampNanos).put("to", timestampNanos);

		JSONObject where = new JSONObject();
		for (Map.Entry<String, ArrayList<String>> tag : tags.entrySet()) {
			where.put(tag.getKey(), new JSONArray(tag.getValue()));
		}

		JSONObject output = new JSONObject().put("format", "csv");

		JSONObject readQuery;
        if (where.length() > 0) {
            readQuery = new JSONObject().put("metric", metric).put("range", range).put("where", where).put("output", output);
        }
        else {
            readQuery = new JSONObject().put("metric", metric).put("range", range).put("output", output);
        }


		HttpPost readRequest = new HttpPost(akumuliHTTPUrl);

		if (_debug) {
			System.out.println("Read Request: " + readQuery.toString());
		}

		if (test) {
			return SUCCESS;
		}

		try {

			readRequest.setEntity(new StringEntity(readQuery.toString()));

			HttpResponse response = akumuliHTTPClient.execute(readRequest);

			String responseStr = EntityUtils.toString(response.getEntity());

			if (_debug) {
				System.out.println('\n' + "Read Response: " + responseStr);
			}

			String[] responseData = responseStr.replace(" ","").split(",");

			if (responseData.length < 3) {
				System.err.println("ERROR: No value found for metric " + metric + ", timestamp " + timestamp.toString()
						+ " and tags " + tags.toString() + ".");
				return -1;
			} else if (responseData.length > 3) {
				System.out.println("Found more than one value for metric " + metric + ", timestamp "
						+ timestamp.toString() + " and tags " + tags.toString() + ".");
				return -1;
			} else if (! timestampNanos.equals(responseData[responseData.length-2])){
                System.out.println("Found value with other timestamp than expected for metric " + metric + "," +
                        " timestamp expected " + timestamp.toString() +  " timestamp received " +
                        responseData[responseData.length-2] + " and tags " + tags.toString() + ".");
                return -1;
            }
            else {
				if (_debug) {
					System.out.println("Found value " + responseData[2].trim() + " for metric " + metric
							+ ", timestamp " + timestamp.toString() + " and tags " + tags.toString() + ".");
				}
			}

		} catch (Exception exc) {
			exc.printStackTrace();
			return -1;
		}

		return SUCCESS;

	}

	@Override
	public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String, ArrayList<String>> tags,
			boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit) {

		if (metric == null || metric.isEmpty() || startTs == null || endTs == null) {
			return -1;
		}
		JSONObject range = new JSONObject().put("from", df.format(startTs)+startTs.getNanos()).put("to",
                df.format(endTs)+endTs.getNanos());

		JSONObject where = new JSONObject();
		for (Map.Entry<String, ArrayList<String>> tag : tags.entrySet()) {
			where.put(tag.getKey(), new JSONArray(tag.getValue()));
		}

		JSONObject sample = new JSONObject();

		// count and sum not supported, for those we will be use max instead
		if (avg) {
			sample.put("name", "paa");
		} else {
			sample.put("name", "max-paa");
		}

		JSONObject groupBy = new JSONObject();
        if (timeValue != 0) {
            switch (timeUnit) {
                case HOURS:
                    groupBy.put("time", timeValue + "h");
                    break;
                case MINUTES:
                    groupBy.put("time", timeValue + "m");
                    break;
                case SECONDS:
                    groupBy.put("time", timeValue + "s");
                    break;
                case MILLISECONDS:
                    groupBy.put("time", timeValue + "ms");
                    break;
                case MICROSECONDS:
                    groupBy.put("time", timeValue + "us");
                    break;
                case NANOSECONDS:
                    groupBy.put("time", timeValue + "n");
                    break;
                default:
                    // time unit not supported => convert to whole nanoseconds,
                    // precision can be lost
                    groupBy.put("time", TimeUnit.NANOSECONDS.convert(timeValue, timeUnit) + "n");
                    break;
            }
        }
        else {
            // if timeValue is zero, one large bucket (bin in Akumuli wiki) is needed
            long timeValueInMs = endTs.getTime() - startTs.getTime();
            timeValueInMs += (timeValueInMs/100)*10; // add 10% for safety
            if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)) {
                groupBy.put("time", TimeUnit.DAYS.convert(endTs.getTime() - startTs.getTime(), TimeUnit.MILLISECONDS) + "d");
            }
            else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS)) {
                groupBy.put("time", TimeUnit.HOURS.convert(endTs.getTime() - startTs.getTime(), TimeUnit.MILLISECONDS) + "h");
            }
            else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)) {
                groupBy.put("time", TimeUnit.MINUTES.convert(endTs.getTime() - startTs.getTime(), TimeUnit.MILLISECONDS) + "m");
            }
            else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS)) {
                groupBy.put("time", TimeUnit.SECONDS.convert(endTs.getTime() - startTs.getTime(), TimeUnit.MILLISECONDS) + "s");
            }
            else {
                groupBy.put("time", TimeUnit.MILLISECONDS.convert(endTs.getTime() - startTs.getTime(), TimeUnit.MILLISECONDS) + "ms");
            }
        }
		JSONObject output = new JSONObject().put("format", "csv");

		JSONObject scanQuery = new JSONObject().put("metric", metric).put("range", range).put("where", where)
				.append("sample", sample).put("group-by", groupBy).put("output", output);

		HttpPost readRequest = new HttpPost(akumuliHTTPUrl);

		if (test) {
			return SUCCESS;
		}

		try {

			readRequest.setEntity(new StringEntity(scanQuery.toString()));

			HttpResponse response = akumuliHTTPClient.execute(readRequest);

			String responseStr = EntityUtils.toString(response.getEntity());

			if (_debug) {
				System.out.println("Scan Request: " + scanQuery.toString() + '\n' + "Scan Response: " + responseStr);
			}

			String[] responseData = responseStr.split(",");

			if (responseData.length < 3) {
				// allowed to happen, no error message
//				System.err.println("ERROR: No value found for metric " + metric + ", start timestamp "
//						+ startTs.toString() + ", end timestamp " + endTs.toString() + ", avg=" + avg + ", count="
//						+ count + ", sum=" + sum + ", time value " + timeValue + ", time unit " + timeUnit
//						+ " and tags " + tags.toString() + ".");
				return -1;
			} else {
				if (_debug) {
					System.out.println("Found value(s) for metric " + metric + ", start timestamp " + startTs.toString()
							+ ", end timestamp " + endTs.toString() + ", avg=" + avg + ", count=" + count + ", sum="
							+ sum + ", time value " + timeValue + ", time unit " + timeUnit + " and tags "
							+ tags.toString() + ".");
				}
			}

		} catch (Exception exc) {
			exc.printStackTrace();
			return -1;
		}

		return SUCCESS;

	}

	@Override
	public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {

		if (metric == null || metric.isEmpty() || timestamp == null) {
			return -1;
		}

		if (test) {
			return SUCCESS;
		}

		StringBuilder insertRequest = new StringBuilder("+");
		insertRequest.append(metric);

		for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
			insertRequest.append(' ');
			insertRequest.append(tag.getKey());
			insertRequest.append('=');
			insertRequest.append(tag.getValue().toString());
		}

		insertRequest.append("\r\n:");
		insertRequest.append(getNanoSecOfTimestamp(timestamp));
		insertRequest.append("\r\n+");
		insertRequest.append(value);
		 insertRequest.append("\r\n");

		if (_debug) {
			System.out.println("Insert Request:\n" + insertRequest);
		}

		// No response, so we doesn't know if new data was accepted /
		// successfully stored => SUCCESS will be always returned
		insertWriter.print(insertRequest);
		 insertWriter.flush();

		if (_debug) {
			System.out.println("Inserted metric " + metric + ", timestamp " + getNanoSecOfTimestamp(timestamp) + ", value " + value
					+ " and tags " + tags + ".");
		}

		return SUCCESS;

	}

}
