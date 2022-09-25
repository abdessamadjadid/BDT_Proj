package cs523.producer;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Producer {

	final Logger logger = LoggerFactory.getLogger(Producer.class);

	// client Read from twitter
	private Client client;
	
	//Kafka Producre created
	private KafkaProducer<String, String> producer;
	
	//every 15 msg producer will send it
	private BlockingQueue<String> twitterMsgQueue = new LinkedBlockingQueue<>(15);
	
	//List of Categories
	private List<String> Terms = Lists.newArrayList("food", "kitchen", "chef", "plates", "spices");

	// Run of producer
	public static void main(String[] args) {
		new Producer().run();
	}

	// Twitter Client will send it to the producer
	private Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		// Setting up a connection
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
		// Term that I want to search on Twitter
		hbEndpoint.trackTerms(Terms);
		// Twitter API and tokens
		Authentication hosebirdAuth = new OAuth1("1R5FaVapm53Gnc0OgijvMmd28",
				"xlggPOkv9C61OcEW9JznROomkKPqPwZxBJAg25DsKwnUT7506y",
				"154526910-OBcHAEnnraYFLBj5YEZlw2KtoUULzROIBnkcorVG",
				"dLnUIaAE2t7jAlzTYNY3dkaSGBIMMtBi3kCbWcUNj6SEZ");

		// Creating a client
		
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client")
				.hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hbEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hbClient = builder.build();

		return hbClient;
	}

	// Kafka Producer
	private KafkaProducer<String, String> createKafkaProducer() {
		
		// Create producer properties
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KafkaConfig.BOOTSTRAPSERVERS);

		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);

		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		
		return new KafkaProducer<String, String>(properties);
	}

	//
	private void run() {
		
		// making a setting up
		logger.info("Setting up");

		// Calling the Twitter Client 
		client = createTwitterClient(twitterMsgQueue);
		client.connect();

		// Creating Kafka Producer 
		producer = createKafkaProducer();

		// Shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Application is not stopping!");
			// stoping the client
			client.stop();
			logger.info("Closing Producer");
			// stoping the producer
			producer.close();
			logger.info("Finished closing");
		}));

		// Sending the Tweets to the Kafka
		while (!client.isDone()) {
			String msg = null;
			
			try {
				// polling the twitter msgs every 3 seconds
				msg = twitterMsgQueue.poll(3, TimeUnit.SECONDS);
				
			}
			
			catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			
			// convert msg to json object
			if (msg != null) {
				logger.info(msg);
				JSONObject jsonObj = new JSONObject(msg);
				logger.info(jsonObj.toString());
				Tweet t = getTweet(jsonObj);

				logger.info(t.toString());

				// sending 
				producer.send(new ProducerRecord<String, String>(
						// Read the Topic
						KafkaConfig.TOPIC, "", new Gson().toJson(t)),
						new Callback() {
							@Override
							public void onCompletion(
									
									RecordMetadata recordMetadata, Exception e) {
								if (e != null) {
									logger.error(
											"Some error OR something bad happened",
											e);
								}
							}
						});
			}
		}
		logger.info("\n Application End");
	}

	// Reading the Tweets coming from the Client
	public static Tweet getTweet(JSONObject o) {

		Tweet tweet = new Tweet();

		tweet.setId(o.getString("id_str"));
		System.out.println("id: " + o.getString("id_str"));
		tweet.setText(o.getString("text"));
		tweet.setRetweet(tweet.getText().startsWith("RT @"));
		tweet.setInReplyToStatusId(o.get("in_reply_to_status_id").toString());

		// store the hashTags
		JSONArray hashTags = o.getJSONObject("entities")
				.getJSONArray("hashtags");

		hashTags.forEach(tag -> {
			tweet.getHashTags().add(tag.toString());
		});

		tweet.setUsername(o.getJSONObject("user").getString("screen_name"));
		tweet.setTimeStamp(o.getString("timestamp_ms"));
		tweet.setLang(o.getString("lang"));

		return tweet;
	}
}