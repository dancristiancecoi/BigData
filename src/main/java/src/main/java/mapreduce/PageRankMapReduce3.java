package mapreduce;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapReduce3 {
	
	static class Mapper3 extends Mapper<Object, Text, Text, Text> {
		Text articleTitle = new Text();
		Text pageRank = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		/*
		 * Final mapper that outputs Article title - page rank pairs. No reducer necessary.  
		 */
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splittedValue = value.toString().split("\t");
			String articleTitle = splittedValue[0];
			String pageRank = splittedValue[1];
			this.articleTitle.set(articleTitle);
			this.pageRank.set(pageRank);
			context.write(this.articleTitle, this.pageRank);
	}
	
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
