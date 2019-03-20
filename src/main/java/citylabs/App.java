package citylabs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

//import org.apache.spark.mllib.feature.Word2VecModel.Word2VecModelReader;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.sql.SparkSession;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws FileNotFoundException, IOException {
		SparkSession spark = SparkSession.builder().appName("Java Spark ").config("spark.master", "local")
				.getOrCreate();
		// Load and parse the data file, converting it to a DataFrame.
		spark.log().info("Hi!");

		File modelFile = new File(args[0]);
		String output = args[1];
		Word2VecModel model;
		spark.log().info(modelFile.getAbsolutePath());

		HashMap<String, float[]> modelData = new HashMap<String, float[]>();

		try (BufferedReader b = new BufferedReader(new FileReader(modelFile))) {
			String line;
			while ((line = b.readLine()) != null) {
				String[] pieces = line.split("\\s+");
				String[] nofloats = pieces[1].split(",");
				float[] floats = new float[nofloats.length];
				for (int i = 0; i < nofloats.length; i++) {
					floats[i] = Float.parseFloat(nofloats[i]);
				}
//				spark.log().info(pieces[0]);
				modelData.put(pieces[0], floats);
			}
		}
		spark.log().info("Map Loaded");
		
		model = new Word2VecModel(toScalaMap(modelData));
		spark.log().info("Model Loaded");
		spark.log().info("Writing to " + output);
		model.save(spark.sparkContext(), output);
		spark.log().info("Model Saved");
	}

	public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(HashMap<A, B> m) {
		return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>>conforms());
	}
}
