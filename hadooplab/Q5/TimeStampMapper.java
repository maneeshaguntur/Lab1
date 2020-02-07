package ques5;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeStampMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String timeStamp = "";
		String url = "";

		if (line.contains("404")) {
			int i = 11;
			while (line.charAt(i) != '[') {
				i++;
			}
			i++;

			while (line.charAt(i) != ']') {
				timeStamp = timeStamp + line.charAt(i);
				i++;
			}
			int count = 0;
			while (count != 3) {
				if (line.charAt(i) == '"') {
					count++;
				}
				i++;
			}
			while (line.charAt(i) != '"') {
				url = url + line.charAt(i);
				i++;
			}
			context.write(new Text(timeStamp), new Text(url));
		}
	}
}
