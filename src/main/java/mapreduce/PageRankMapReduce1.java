package mapreduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankMapReduce1 {
	
	static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		
		//ArticlePageRankWritable articlePageRank = new ArticlePageRankWritable();
		Text articleTitle = new Text();
		Text outlink = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		/*
		 * Page Rank Map 1 creates  [<Page title, Page Rank> - Outlink] pairs.
		 * The PageRank is initialised to 1. 
		 */
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lines = value.toString().split("\n");
			
			for (String line : lines) {
				
				if (line.contains("REVISION")) {
					String[] lineSplitted = line.split(" ");
					this.articleTitle.set(lineSplitted[3]);
				}
				
				if (line.contains("MAIN")) {
					String[] lineSplitted = line.split(" ");
					
					if (lineSplitted.length == 1) {
						this.outlink.set("");
						context.write(articleTitle, this.outlink);
					}
					
					for(int i = 1; i<lineSplitted.length; i++) {						
						String outlink = lineSplitted[i];
						this.outlink.set(outlink);
						context.write(this.articleTitle, this.outlink);
				}
					
					
					
				}
				
				
				
			}
		}

	}
	/*
	 * Reducer combines all the (<Article Title, Page Rank> - Outlink)  pairs into (<Article Title, Page Rank> - [Outlinks])
	 * Self loops are removed and only unique outlinks are added to the Outlinks list. 
	 */
	static class Reducer1 extends Reducer<Text ,Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder outlinks = new StringBuilder();
			
			boolean isFirst = true;
			String articleTitle = key.toString();
			
			for(Text outlink : values) {
						
				//self loops are removed
				if(articleTitle.equals(outlink)) {
					continue;
				}
		
				//only unique outlinks are added. 
				if(!outlinks.toString().contains(outlink.toString())) {
					
					//check to remove the trailing commas. 
					if(!isFirst) {
						outlinks.append(" ");
					}
					outlinks.append(outlink.toString());
					isFirst = false;
				}
			}
			context.write(new Text(articleTitle.concat("\t1.0")), new Text(outlinks.toString()));


			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
