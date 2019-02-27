package citylabs;

import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Java Spark ").config("spark.master", "local").getOrCreate();
		// Load and parse the data file, converting it to a DataFrame.
		spark.log().info("Hi!");
	}
}
