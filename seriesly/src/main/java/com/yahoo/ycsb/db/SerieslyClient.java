package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * Seriesly client for YCSB framework. It's possible to store tags, as seriesly
 * is schemaless. But the filtering in seriesly doesn't work as the framework
 * expects it.
 * 
 * @author Michael Zimmermann
 */
public class SerieslyClient extends DB {

	private boolean _debug = false;
	private boolean test = false;
	private final int SUCCESS = 0;

	private String db_name = "TestDB";
	private String metricFieldName = "metric";
	private String valueFieldName = "value";
	private String tagsFieldName = "tags";

	private String ip = "localhost";
	private int port = 3133;
	private int retries = 3;
	private CloseableHttpClient client;

	private URL db_url = null;

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() throws DBException {

		try {
			test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
			
			if (!getProperties().containsKey("port") && !test) {
				throw new DBException("No port given, abort.");
			}
			port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
			
			if (!getProperties().containsKey("ip") && !test) {
				throw new DBException("No ip given, abort.");
			}
			ip = getProperties().getProperty("ip", ip);


			if (_debug) {
				System.out.println("The following properties are given: ");
				for (String element : getProperties().stringPropertyNames()) {
					System.out.println(element + ": " + getProperties().getProperty(element));
				}
			}

			RequestConfig requestConfig = RequestConfig.custom().build();
			if (!test) {
				client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
			}

		} catch (Exception e) {
			throw new DBException(e);
		}

		try {
			db_url = new URL("http", ip, port, "/" + db_name);
			if (_debug) {
				System.out.println("URL: " + db_url);
			}

		} catch (MalformedURLException e) {
			throw new DBException(e);
		}

		if (doPut(db_url) != HttpURLConnection.HTTP_CREATED) {
			throw new DBException("Error creating the DB.");
		}
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
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

	private Integer doPut(URL url) {

		Integer statusCode;
		HttpResponse response = null;
		try {
			HttpPut putMethod = new HttpPut(url.toString());

			int tries = retries + 1;
			while (true) {
				tries--;
				try {
					response = client.execute(putMethod);
					break;
				} catch (IOException e) {
					if (tries < 1) {
						System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + " times.");
						e.printStackTrace();
						if (response != null) {
							EntityUtils.consumeQuietly(response.getEntity());
						}
						putMethod.releaseConnection();
						return null;
					}
				}
			}

			statusCode = response.getStatusLine().getStatusCode();
			EntityUtils.consumeQuietly(response.getEntity());
			putMethod.releaseConnection();

		} catch (Exception e) {
			System.err.println("ERROR: Error while trying to send PUT request '" + url.toString() + "'.");
			e.printStackTrace();
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
			return null;
		}

		return statusCode;
	}

	private Integer doPost(URL url, String queryStr) {

		Integer statusCode;
		HttpResponse response = null;
		try {
			HttpPost postMethod = new HttpPost(url.toString());
			StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
			postMethod.setEntity(requestEntity);

			int tries = retries + 1;
			while (true) {
				tries--;
				try {
					response = client.execute(postMethod);
					break;
				} catch (IOException e) {
					if (tries < 1) {
						System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + " times.");
						e.printStackTrace();
						if (response != null) {
							EntityUtils.consumeQuietly(response.getEntity());
						}
						postMethod.releaseConnection();
						return null;
					}
				}
			}

			statusCode = response.getStatusLine().getStatusCode();
			EntityUtils.consumeQuietly(response.getEntity());
			postMethod.releaseConnection();

		} catch (Exception e) {
			System.err.println("ERROR: Errror while trying to query " + url.toString() + " for '" + queryStr + "'.");
			e.printStackTrace();
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
			return null;
		}

		return statusCode;
	}

	private JSONObject doGet(URL url) {

		JSONObject jsonObject = new JSONObject();
		HttpResponse response = null;
		try {
			HttpGet getMethod = new HttpGet(url.toString());
			getMethod.addHeader("accept", "application/json");

			int tries = retries + 1;
			while (true) {
				tries--;
				try {
					response = client.execute(getMethod);
					break;
				} catch (IOException e) {
					if (tries < 1) {
						System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + " times.");
						e.printStackTrace();
						if (response != null) {
							EntityUtils.consumeQuietly(response.getEntity());
						}
						getMethod.releaseConnection();
						return null;
					}
				}
			}

			if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {

				BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				StringBuilder builder = new StringBuilder();
				String line;
				while ((line = bis.readLine()) != null) {
					builder.append(line);
				}

				if (builder.length() > 0) {
					jsonObject = new JSONObject(builder.toString());
				}
			}
			EntityUtils.consumeQuietly(response.getEntity());
			getMethod.releaseConnection();

		} catch (Exception e) {
			System.err.println("ERROR: Errror while trying to query " + url.toString() + ".");
			e.printStackTrace();
			if (response != null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
			return null;
		}

		return jsonObject;
	}

	/**
	 * Read a record from the database. Each value from the result will be
	 * stored in a HashMap
	 *
	 * @param metric
	 *            The name of the metric
	 * @param timestamp
	 *            The timestamp of the record to read.
	 * @param tags
	 *            actual tags that were want to receive (can be empty)
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {
		if (metric == null || metric == "") {
			return -1;
		}
		if (timestamp == null) {
			return -1;
		}

		try {
			long timestampLong = timestamp.getTime();
			String urlStr = String.format("%s/_query?from=%s&to=%s&group=1&ptr=/%s&reducer=any&f=/%s&fv=%s", db_url,
					timestampLong, timestampLong, valueFieldName, metricFieldName, metric);
			URL newQueryURL = new URL(urlStr);

			if (_debug) {
				System.out.println("QueryURL: " + newQueryURL.toString());
			}

			JSONObject jsonObject = doGet(newQueryURL);

			if (_debug) {
				System.out.println("Answer: " + jsonObject.toString());
			}

			JSONArray jsonArray = jsonObject.getJSONArray(Long.toString(timestampLong));

			if (jsonArray == null || jsonArray.length() < 1) {

				System.err.println("ERROR: Found no values for metric: " + metric + ".");
				return -1;

			}

		} catch (MalformedURLException e) {
			System.err.println("ERROR: a malformed URL was generated.");
			return -1;
		} catch (Exception e) {
			return -1;
		}

		if (test) {
			return SUCCESS;
		}

		return SUCCESS;

	}

	/**
	 * Perform a range scan for a set of records in the database. Each value
	 * from the result will be stored in a HashMap.
	 *
	 * @param metric
	 *            The name of the metric
	 * @param startTs
	 *            The timestamp of the first record to read.
	 * @param endTs
	 *            The timestamp of the last record to read.
	 * @param tags
	 *            actual tags that were want to receive (can be empty)
	 * @param avg
	 *            do averageing
	 * @param sum
	 *            do summarize
	 * @param count
	 *            do count
	 * @param timeValue
	 *            value for timeUnit for sum/count/avg
	 * @param timeUnit
	 *            timeUnit for sum/count/avg
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String, ArrayList<String>> tags,
			boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit) {
		if (metric == null || metric == "") {
			return -1;
		}
		if (startTs == null || endTs == null) {
			return -1;
		}

		String aggregation = "any";
		if (avg) {
			aggregation = "avg";
		} else if (count) {
			aggregation = "count";
		} else if (sum) {
			aggregation = "sum";
		}

		Integer grouping;
		if (timeUnit == TimeUnit.MILLISECONDS) {
			grouping = 1;
		} else if (timeUnit == TimeUnit.SECONDS) {
			grouping = 1000;
		} else if (timeUnit == TimeUnit.MINUTES) {
			grouping = 60000;
		} else if (timeUnit == TimeUnit.HOURS) {
			grouping = 3600000;
		} else if (timeUnit == TimeUnit.DAYS) {
			grouping = 86400000;
		} else {
			System.err.println(
					"WARNING: Timeunit " + timeUnit.toString() + " is not supported. Using milliseconds instead!");
			grouping = 1;
		}

		grouping = grouping * timeValue;

		try {
			String urlStr = String.format("%s/_query?from=%s&to=%s&group=%s&ptr=/%s&reducer=%s&f=/%s&fv=%s", db_url,
					startTs.getTime(), endTs.getTime(), grouping, valueFieldName, aggregation, metricFieldName, metric);
			URL newQueryURL = new URL(urlStr);

			if (_debug) {
				System.out.println("QueryURL: " + newQueryURL.toString());
			}

			JSONObject jsonObject = doGet(newQueryURL);

			if (_debug) {
				System.out.println("Answer: " + jsonObject.toString());
			}

			String[] elementNames = JSONObject.getNames(jsonObject);

			JSONArray jsonArray = jsonObject.getJSONArray(elementNames[0]);

			if (jsonArray == null || jsonArray.length() < 1) {

				System.err.println("ERROR: Found no values for metric: " + metric + ".");
				return -1;

			}

		} catch (MalformedURLException e) {
			System.err.println("ERROR: a malformed URL was generated.");
			return -1;
		} catch (Exception e) {
			return -1;
		}

		if (test) {
			return SUCCESS;
		}

		return SUCCESS;

	}

	/**
	 * Insert a record in the database. Any tags/tagvalue pairs in the specified
	 * tags HashMap and the given value will be written into the record with the
	 * specified timestamp
	 *
	 * @param metric
	 *            The name of the metric
	 * @param timestamp
	 *            The timestamp of the record to insert.
	 * @param value
	 *            actual value to insert
	 * @param tags
	 *            A HashMap of tag/tagvalue pairs to insert as tags
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {
		if (metric == null || metric == "") {
			return -1;
		}
		if (timestamp == null) {
			return -1;
		}

		try {

			JSONObject insertObject = new JSONObject();
			insertObject.put(metricFieldName, metric);
			insertObject.put(valueFieldName, value);

			if (tags != null && !tags.isEmpty()) {

				JSONObject tagsObject = new JSONObject();
				for (Entry<String, ByteIterator> entry : tags.entrySet()) {
					tagsObject.put(entry.getKey(), entry.getValue().toString());
				}
				insertObject.put(tagsFieldName, tagsObject);

			}

			String query = insertObject.toString();

			String urlStr = String.format("%s?ts=%s", db_url.toString(), timestamp.getTime());
			URL insertURL = new URL(urlStr);

			if (_debug) {
				System.out.println("Insert measures String: " + query);
				System.out.println("Insert measures URL: " + insertURL.toString());
			}
			if (test) {
				return SUCCESS;
			}
			Integer statusCode = doPost(insertURL, query);

			if (_debug) {
				System.out.println("StatusCode: " + statusCode);
			}
			if (statusCode != HttpURLConnection.HTTP_CREATED) {
				System.err.println("ERROR: Error in processing insert to metric: " + metric);
				return -1;
			}
			return SUCCESS;

		} catch (Exception e) {
			System.err.println("ERROR: Error in processing insert to metric: " + metric);
			e.printStackTrace();
			return -1;
		}
	}

}
