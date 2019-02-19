package mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ArticlePageRankWritable implements WritableComparable<ArticlePageRankWritable>{
	private String articleTitle;
	private double pageRank;
	
	@Override
	public void readFields(DataInput in) throws IOException{
		articleTitle = in.readUTF();
		pageRank = in.readDouble();
	}
	
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeUTF(articleTitle);
		out.writeDouble(pageRank);
	}
	
	public static ArticlePageRankWritable read(DataInput in) throws IOException{
		ArticlePageRankWritable w = new ArticlePageRankWritable();
		w.readFields(in);
		return w;
	}
	
//	@Override
//	public int compareTo(ArticlePageRankWritable o) {
//		return this.articleTitle.compareTo(o.articleTitle);	
//	}

	@Override
	public int compareTo(ArticlePageRankWritable o) {
		return this.articleTitle.compareTo(o.articleTitle);
	}
	
	public void setArticle(String articleTitle) {
		this.articleTitle = articleTitle;
	}
	
	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}
	
	public String getArticle() {
		return this.articleTitle;
	}
	
	public double getPageRank() {
		return this.getPageRank();
	}
	

	

}




