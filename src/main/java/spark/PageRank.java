package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class PageRank {
	
//	public static String INPUT_PATH = "";
//	public static String OUTPUT_PATH = "";
//	public static int ITERATIONS = 5;
//	public static long TIMESTAMP = 0;
	


	public static void main(String[] args) throws Exception {
		//  check for valid parameters (needs to be changed to 4 when Date/Time filter implemented)
		if (args.length!=4) {

			System.out.println("\n ERROR: Incorrect number of parameters have been supplied \n");
			System.exit(0);
		}
		
		String INPUT_PATH = "";
		String OUTPUT_PATH = "";
		int ITERATIONS = 5;
		long TIMESTAMP = utils.ISO8601.toTimeMS(args[3]);
		
		try {
			INPUT_PATH = args[0];
			OUTPUT_PATH = args[1];
			ITERATIONS = Integer.parseInt(args[2]);
//			TIMESTAMP = utils.ISO8601.toTimeMS(args[3]);
		}
		
		catch (Exception e) {
			System.out.println("\n ERROR: Invalid parameters have been supplied \n");
			System.exit(0);
		}
		
        // delete output path if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(OUTPUT_PATH)))
            fs.delete(new Path(OUTPUT_PATH), true);
        
        
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank"));
		sc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
		
		JavaRDD<String> lines = sc.textFile(INPUT_PATH);
		

		JavaPairRDD<String, Tuple2<Long, String>> links = lines.mapToPair( (String line) -> {
			String article = "";
			StringBuilder outlinks = new StringBuilder();
			long timestamp = 0;
			for (String l: line.split("\n")) {
				if(l.startsWith("REVISION")) {
					String[] lineSplitted = l.split(" ");
					article = lineSplitted[3]; 
					try {
						timestamp = utils.ISO8601.toTimeMS(lineSplitted[4]);
					} catch (ParseException e) {
						timestamp = 0;
					}
				}	
		
				if (article.equals("") || timestamp >= TIMESTAMP) {
					return new Tuple2<String, Tuple2<Long, String>>("", new Tuple2<Long, String>((long) 0, ""));
				}
				

				if(l.startsWith("MAIN")) {
					String[] lineSplitted = l.split(" ");
					// no outlinks
					if(lineSplitted.length == 1) {
						return new Tuple2<String, Tuple2<Long, String>>(article, new Tuple2<Long, String>(timestamp, ""));
					}
//					
					
					for(int i = 1; i<lineSplitted.length; i++) {
						// ignore self loops
						if (lineSplitted[i] == article) {
							continue;
						}
						// ignore duplicating outlinks
						if (!outlinks.toString().contains(lineSplitted[i])) {
							outlinks.append(lineSplitted[i]).append(" ");
						}
					}
				}
			}
			
			return new Tuple2<String, Tuple2<Long, String>>(article, new Tuple2<Long, String>(timestamp, outlinks.toString()));
		
			}).reduceByKey((a,b) -> {
				// get most recent timestamp
				long a_time = a._1;
				long b_time = b._1;
	
				return a_time > b_time ? a : b;
				
			});
		
//		links.saveAsTextFile("spark-input");
//		System.out.println(links.count());
		
		// Initialises pagerank 1
		JavaPairRDD<String, Double> ranks = links.mapValues(s -> 1.0);
		
//		ranks.saveAsTextFile("spark-rank");
		
		for(int i = 0; i < ITERATIONS; i++) {
			JavaPairRDD<String, Double> contribs = links.join(ranks).values()
				.flatMapToPair(v -> {
					List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
					// <article, <timestamp, outlinks, pr>>
					String outlinkString = v._1._2;
					String[] outlinks = outlinkString.split("\\s+");
					int urlCount = outlinks.length;
					for (String outlink : outlinks) {
						res.add(new Tuple2<String, Double>(outlink, v._2() / urlCount));
					}
					return res;
				});
				ranks = contribs.reduceByKey((a, b) -> a+b).mapValues(v -> 0.15 + v * 0.85);
		}
		ranks.saveAsTextFile(OUTPUT_PATH);
		List<Tuple2<String, Double>> output = ranks.collect();
//		System.out.println(output);
	}
}
