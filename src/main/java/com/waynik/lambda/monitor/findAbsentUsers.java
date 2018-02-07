package com.waynik.lambda.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;

import java.sql.SQLException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.gson.Gson;


public class FindAbsentUsers implements RequestHandler<Object, Integer> {

	private long ONE_DAY = 24 * 60 * 60 * 1000L;
	private long TWO_DAYS = 2 * ONE_DAY;
	private String ONE_DAY_TOPIC = "arn:aws:sns:no_checkin_24_hours";
	private String TWO_DAYS_TOPIC = "arn:aws:sns::no_checkin_48_hours";

	private String WAYNIK_EVENTS_SECRET_KEY = "secret";
	private String AWS_SNS_KEY = "";
	private String AWS_SNS_SECRET = "/8";
	private String AWS_REGION_CODE = "us-west-2";

	private LambdaLogger logger;

    @Override
    public Integer handleRequest(Object input, Context context) {
    	logger = context.getLogger();
    	logger.log("Input: " + input + "\n");

    	String url = "jdbc:mysql://mysql.com";
    	String userName = "web";
    	String password = "";

    	Connection connection = null;
    	Statement stmt = null;
    	ResultSet resultSet = null;
    	PreparedStatement statement24 = null;
    	PreparedStatement statement48 = null;

    	try {
			connection = DriverManager.getConnection(url, userName, password);
			stmt = connection.createStatement();
	        resultSet = stmt.executeQuery("SELECT id, user_id, created_at, 24_hour_warning, 48_hour_warning FROM checkins WHERE id IN ( SELECT MAX(id) FROM checkins GROUP BY user_id);");

	        while (resultSet.next()) {
				int checkinId = resultSet.getInt("id");
				int userId = resultSet.getInt("user_id");
				logger.log("checking user: " + userId + "\n");
				boolean oneDayWarning = resultSet.getBoolean("24_hour_warning");
				boolean twoDayWarning = resultSet.getBoolean("48_hour_warning");
				Date createdAt = resultSet.getDate("created_at");

				if (olderThanOneDay(createdAt) && !oneDayWarning) {
					logger.log("sending 24 hour event for " + userId + "\n");

					statement24 = connection.prepareStatement("UPDATE checkins SET 24_hour_warning = 1 WHERE id = ?;");
					statement24.setString(1, Integer.toString(checkinId));
					statement24.execute();

					Map<String, String> data = new HashMap<>();
					data.put("frequency", "1800"); // set frequency to 30 minutes
					PublishResult result = publish(ONE_DAY_TOPIC, userId, data);
					logger.log("MessageId - " + result.getMessageId() + "\n");
				}

				if (olderThanTwoDays(createdAt) && !twoDayWarning) {
					logger.log("sending 48 hour event for " + userId + "\n");

					statement48 = connection.prepareStatement("UPDATE checkins SET 48_hour_warning = 1 WHERE id = ?;");
					statement48.setString(1, Integer.toString(checkinId));
					statement48.execute();

					PublishResult result = publish(TWO_DAYS_TOPIC, userId, "We haven't heard from you in 2 days! Open the app and start the service.");
					logger.log("MessageId - " + result.getMessageId() + "\n");
				}
	        }

    	} catch (SQLException e) {
			logger.log(e.getMessage() + "\n");
		} finally {
			DbUtils.closeQuietly(resultSet);
			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(statement24);
			DbUtils.closeQuietly(statement48);
			DbUtils.closeQuietly(connection);
		}

        return 1;
    }

    private boolean olderThanOneDay(Date aDate) {
        return aDate.getTime() < System.currentTimeMillis() - ONE_DAY;
    }

    private boolean olderThanTwoDays(Date aDate) {
        return aDate.getTime() < System.currentTimeMillis() - TWO_DAYS;
    }

    private PublishResult publish(String topic, int userId, String message) {
        String json = getJsonPayload(userId, message);
        logger.log(json + "\n");
        return sendSnsRequest(topic, json);
    }

    private PublishResult publish(String topic, int userId, Map<String, String> data) {
    	String json = getJsonPayload(userId, data);
        logger.log(json + "\n");
        return sendSnsRequest(topic, json);
    }

    private PublishResult sendSnsRequest(String topic, String json) {
        AmazonSNS snsClient = AmazonSNSClient.builder()
				.withRegion(AWS_REGION_CODE)
				.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(AWS_SNS_KEY, AWS_SNS_SECRET)))
				.build();
		PublishRequest publishRequest = new PublishRequest(topic, json);
		PublishResult publishResult = snsClient.publish(publishRequest);
		//print MessageId of message published to SNS topic
		return publishResult;
    }

    private String getJsonPayload(int userId, String message) {
        Gson gson = new Gson();
        Map<String, String> payload = new HashMap<>();
    	payload.put("apiKey", WAYNIK_EVENTS_SECRET_KEY);
		payload.put("userId", Integer.toString(userId));
		payload.put("message", message);

		return gson.toJson(payload);
    }

    private String getJsonPayload(int userId, Map<String,String> data) {
        Gson gson = new Gson();
        Map<String, Object> payload = new HashMap<>();
    	payload.put("apiKey", WAYNIK_EVENTS_SECRET_KEY);
		payload.put("userId", Integer.toString(userId));
		payload.put("data", data);

		return gson.toJson(payload);
    }
}
