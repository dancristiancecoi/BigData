package mapreduce;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankMapReduce2 {
	
	static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		
		Text articleTitle = new Text();
		Text outlinkList = new Text();
		Text outlink = new Text();
		Text pageRank = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		/*
		 * Page Rank Map 1 creates  [<Page title, Page Rank> - Outlink] pairs.
		 * The PageRank is initialised to 1. 
		 */
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splittedValue = value.toString().split("\t");
			String articleTitle = splittedValue[0];
			String pageRank = splittedValue[1];
			
			if(splittedValue.length==3) {
				String[] outlinkList = splittedValue[2].split("\\s+");
				for(String outlink : outlinkList) {
					
					this.outlink.set(outlink);
					float pageRankFloat = Float.parseFloat(pageRank)/outlinkList.length;
					this.pageRank.set(Float.toString(pageRankFloat));
					context.write(this.outlink, this.pageRank);
				}
				
				this.outlinkList.set(splittedValue[2]);

			}else {
				this.outlinkList.set("");
			}
					
			this.articleTitle.set(articleTitle);
			context.write(this.articleTitle, this.outlinkList);

		}

	}
	
	/*
	 * Reducer combines all the (<Article Title, Page Rank> - Outlink)  pairs into (<Article Title, Page Rank> - [Outlinks])
	 * Self loops are removed and only unique outlinks are added to the Outlinks list. 
	 */
	static class Reducer2 extends Reducer<Text ,Text, Text, Text> {
		
		Text articleTitle = new Text();
		Text pageRank = new Text();
		Text outlinks = new Text();
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// something
			float pageRank = 0;
			
			for(Text value : values) {
				try {
					float number = Float.parseFloat(value.toString());
					pageRank += number;
				}
				catch(Exception e){
					this.outlinks.set(value);
				}
			}
			
			pageRank = (float) (0.15 + 0.85 * pageRank); 
			this.articleTitle.set(key.toString().concat("\t").concat(Float.toString(pageRank)));
			this.pageRank.set(Float.toString(pageRank));
			
			context.write(this.articleTitle, this.outlinks);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
