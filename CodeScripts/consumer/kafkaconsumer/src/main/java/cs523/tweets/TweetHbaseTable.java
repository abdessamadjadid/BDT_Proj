package cs523.tweets;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;


public class TweetHbaseTable {
	
	public static Configuration config = HBaseConfiguration.create();
	public static Connection connection = null;
	
	public static Admin admin = null;
	
	private static final String TABLE_NAME = "food";
	private static final String CF_DEFAULT = "tweet-info";
	private static final String CF_GENERAL = "general-info";

	private final static byte[] CF_DEFAULT_BYTES = CF_DEFAULT.getBytes();
	private final static byte[] CF_GENERAL_BYTES = CF_GENERAL.getBytes();
		
	private static Table food;
	
	static {
		try {
			connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();

			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT)
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_GENERAL)
					.setCompressionType(Algorithm.NONE));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			food = connection.getTable(TableName.valueOf(TABLE_NAME));

			System.out.println(" Done!");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void populateData(Tweet tweet) throws IOException {
		Put row = new Put(tweet.getId().getBytes());

		row.addColumn(CF_DEFAULT_BYTES, "text".getBytes(), tweet.getText()
				.getBytes());
		row.addColumn(CF_DEFAULT_BYTES, "hashtags".getBytes(),
				String.join(", ", tweet.getHashTags()).getBytes());
		row.addColumn(CF_DEFAULT_BYTES, "is_retweet".getBytes(), String
				.valueOf(tweet.isRetweet()).getBytes());

		if (tweet.getInReplyToStatusId() != null && "null".equals(tweet.getInReplyToStatusId()))
			row.addColumn(CF_DEFAULT_BYTES, "reply_to".getBytes(), tweet
					.getInReplyToStatusId().getBytes());

		row.addColumn(CF_GENERAL_BYTES, "username".getBytes(), tweet
				.getUsername().getBytes());
		row.addColumn(CF_GENERAL_BYTES, "timestamp_ms".getBytes(), tweet
				.getTimeStamp().getBytes());
		row.addColumn(CF_GENERAL_BYTES, "lang".getBytes(), tweet.getLang()
				.getBytes());

		food.put(row);
	}
}