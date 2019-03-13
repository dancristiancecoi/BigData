package spark;

import java.util.Arrays;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PageRank {
	static long TIMESTAMP = 0;


	public static void main(String[] args) throws Exception {
		//  check for valid parameters (needs to be changed to 4 when Date/Time filter implemented)
		if (args.length!=4) {

			System.out.println("\n ERROR: Incorrect number of parameters have been supplied \n");
			System.exit(0);
		}
		
		int ITERATIONS = 5;
//		long TIMESTAMP = 0;
		String out = args[1];
		
		try {
			ITERATIONS = Integer.parseInt(args[2]);
			TIMESTAMP = utils.ISO8601.toTimeMS(args[3]);
		}
		
		catch (Exception e) {
			System.out.println("\n ERROR: Invalid parameters have been supplied \n");
			System.exit(0);
		}
		
        // delete output path if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(out)))
            fs.delete(new Path(out), true);
        
        
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank"));
		sc.hadoopConfiguration().set("textinputformat.record.delimeter", "\n\n");
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		
		
		JavaPairRDD<String, String> line = lines.mapToPair( (String s) -> {
			String article = "";
			String outlinks = "";
			String timestamp = "";
			
			for (String l : s.split("\n")) {
				if(l.contains("REVISION")) {
					String[] lineSplitted = l.split(" ");
					article = lineSplitted[3]; 
					timestamp = (lineSplitted[4]);
				}
				
//				if(timestamp <= TIMESTAMP) {
//					return new Tuple2<String, String>("", "");
//				}
				
				if(l.contains("MAIN")) {
					String[] lineSplitted = l.split(" ");
					if(lineSplitted.length == 1) {
						outlinks = "";
					}
					
					else {
						for(int i = 1; i<lineSplitted.length; i++) {
							// ignore self loops
							if (lineSplitted[i] == article) {
								continue;
							}
							// ignore duplicating outlinks
							if (!outlinks.contains(lineSplitted[i])) {
								outlinks += lineSplitted[i] + " ";
							}
						}
					}
				}
			}
			return new Tuple2<String, String>(article, timestamp + " " + outlinks);
			}).reduceByKey((a,b) -> {
				String a_time = a.split("\\s+")[0];
				String b_time = b.split("\\s+")[0];
				System.out.println(a_time);
				System.out.println(b_time);
				if (utils.ISO8601.toTimeMS(a_time) > utils.ISO8601.toTimeMS(b_time)) {
					return a;
				} else {
					return b;
				}
				
			});
		
		System.out.println(out);
		line.saveAsTextFile(out);
	}
}

/**
spark-submit --master yarn --deploy-mode cluster --class spark.PageRank /users/level4/2208414a/eclipse-workspace/BigData/target/uog-bigdata-0.0.1-SNAPSHOT.jar /user/enwiki/enwiki-20080103-sample.txt spark_pr0 1
**/