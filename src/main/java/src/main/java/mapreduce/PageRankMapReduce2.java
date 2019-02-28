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
		 * Page Rank Map 2 takes [<Page title, Page Rank> - Outlink] pairs and new the PageRank.
		 * 
		 */
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * The structure of the input to the second mappers is as follows:
			 * <ArticleTitle PageRank> - OutlinkList
			 * The Article title and page rank are separated by a tab.
			 * The outlinks in the outlink list are separated by spaces 
			 * 
			 * The mapper has two different outputs:
			 * 1) outlink - pageRank 
			 * 2) article title - outlinks 
			 * 
			 */
			
			String[] splittedValue = value.toString().split("\t");
			String articleTitle = splittedValue[0];
			String pageRank = splittedValue[1];
			
			if(splittedValue.length==3) {
				
				String[] outlinkList = splittedValue[2].split("\\s+");
				
				for(String outlink : outlinkList) {
					
					this.outlink.set(outlink);
					float pageRankFloat = Float.parseFloat(pageRank)/outlinkList.length;
					this.pageRank.set(" ".concat(Float.toString(pageRankFloat)));
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
	 * The reducer has two Inputs:
	 * 1) outlink - pageRank 
	 * 2) article title - outlinks
	 * 
	 * In the reducer is where the PageRank formula is applied. 
	 * 
	 *  
	 */
	static class Reducer2 extends Reducer<Text ,Text, Text, Text> {
		
		Text articleTitle = new Text();
		Text pageRank = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			float pageRank = 0;
			Text outlinks = new Text();
			
			for (Text value: values) {
				
				String stringValue = value.toString();
				
				if (stringValue.length() > 0) {
					
					char firstChar = value.toString().charAt(0);
					
					// The reducer gets two different inputs. 
					if (firstChar == ' ') {
						
						float number = Float.parseFloat(value.toString());
						pageRank += number;
					
					}else
						outlinks.set(value);
				}
			}
			
			
			pageRank = (float) (0.15 + 0.85 * pageRank); 
			this.articleTitle.set(key.toString().concat("\t").concat(Float.toString(pageRank)));
			context.write(this.articleTitle, outlinks);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
