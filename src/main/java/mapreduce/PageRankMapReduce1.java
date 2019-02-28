package mapreduce;


import java.io.IOException;
import java.text.ParseException;
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
		Configuration conf;
		long timestampUser;
		long timestampLine;
		Text article = new Text();
		Text outlink = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			conf = context.getConfiguration();
			String timestampS = conf.get("instance_of_timestamp");
			this.timestampUser = Long.parseLong(timestampS);
		}
		
		/*
		 * Page Rank Map 1 creates  Page Rank - Outlink key-value pairs.
		 */
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lines = value.toString().split("\n");
			for (String line : lines) {
				if (line.contains("REVISION")) {
					
					String[] lineSplitted = line.split(" ");
					
					try {
						
						this.timestampLine = utils.ISO8601.toTimeMS(lineSplitted[4]);
						
					} catch (ParseException e) {
						
						throw new IOException(e);
					}
					
					this.article.set(lineSplitted[3]);
					
				}
				//dont do anything if the time stamp of the record is smaller than the timestamp of the user. 			
				if(this.timestampLine < this.timestampUser) {
					
					if (line.contains("MAIN")) {
						
						String[] splittedLine = line.split(" ");
						
						if (splittedLine.length == 1) {
							//There are no outlinks.						
							this.outlink.set("".concat("\t").concat(Long.toString(timestampLine)));
							context.write(this.article, this.outlink);
						}
						
						for(int i = 1; i<splittedLine.length; i++) {						
							String outlink = splittedLine[i];
							this.outlink.set(outlink.concat("\t").concat(Long.toString(timestampLine)));
							context.write(this.article, this.outlink);
						
						
						}	
					}
				}
			}
		}
	}
	
	/*
	 * Reducer combines all the Article Title - Outlink pairs into <Article Title, Page Rank> - [Outlinks]
	 * Self loops are removed and only unique outlinks are added to the Outlinks list. 
	 * Page Rank is initialised to 1. 
	 */
	static class Reducer1 extends Reducer<Text ,Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			
			StringBuilder outlinks = new StringBuilder();
			long previousTimeStamp = Long.MIN_VALUE; 
			
			
			boolean isFirst = true;
			String articleTitle = key.toString();
			
			for(Text value : values) {
				
				String[] splittedValue = value.toString().split("\t");
				String outlink = "";
				long timeStamp;
				
				if(splittedValue.length == 2) {
					outlink = splittedValue[0];
					timeStamp = Long.parseLong(splittedValue[1]);
				}else {
//					timeStamp = Long.parseLong(splittedValue[0]);
	
					continue;
				}
				
				//self loops are removed
				if(articleTitle.equals(outlink)) {
					continue;
				}
				
				if(timeStamp < previousTimeStamp) {
					continue;	
				}
								
				else if(timeStamp == previousTimeStamp) {
					
					//only unique outlinks are added. 
					if(!outlinks.toString().contains(outlink.toString())) {
						
						//check to remove the trailing commas. 
						if(!isFirst) {
							outlinks.append(" ");
						}
						outlinks.append(outlink.toString());
						isFirst = false;
					}
				}else {
					outlinks = new StringBuilder();
					outlinks.append(outlink.toString());
					isFirst = false; 
				}
				previousTimeStamp = timeStamp; 
			}
			
			context.write(new Text(articleTitle.concat("\t1.0")), new Text(outlinks.toString()));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
