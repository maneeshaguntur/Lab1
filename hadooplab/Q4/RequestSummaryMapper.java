package ques4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RequestSummaryMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		int start = line.indexOf('[');
		int stop = line.indexOf(']');
		String timeStr = line.substring(start + 1, stop - 1);
		String month = timeStr.substring(3, 6);
		String year = timeStr.substring(7, 11);
		String combined = month + "-" + year;

		Summary summary = new Summary();
		summary.setRequests(new IntWritable(1));
		int bytes = 0;
		if (line.contains("GET")) {
			String temp = "";
			String pattern = "HTTP/1.1\\\" \\d{3}\\ (\\d+)";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(line);

			if (!m.find()) {
				temp = "0";
			} else {
				temp = m.group(1);
			}
			bytes = Integer.parseInt(temp);

		}
		summary.setDownloadSize(new IntWritable(bytes));
		context.write(new Text(combined), new Text(summary.toString()));
	}
}
