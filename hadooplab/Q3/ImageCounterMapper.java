package ques2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		//String imageType = "";
		if (line.contains("GET") && line.contains("images")) {
			if (line.contains(".jpg")) {
				imageType = "JPG";
			} else if (line.contains(".gif")) {
				imageType = "GIF";
			} else {
				imageType = "OTHERS";
			}
		}
		context.write(new Text(imageType), new IntWritable(1));
	}
}
