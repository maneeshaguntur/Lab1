package ques4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RequestSummaryReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int counter = 0;
		int downloadSize = 0;
		Summary temp;
		for (Text value : values) {
			String str = value.toString();
			int index = str.indexOf(',');
			int dls = Integer.parseInt(str.substring(index + 1));
			counter += 1;
			downloadSize += (dls);
		}
		temp = new Summary(new IntWritable(counter), new IntWritable(downloadSize));
		context.write(key, new Text(temp.toString()));
	}
}
