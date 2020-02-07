package ques2;
//ImageCounter
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImageCounter {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: ImageCounter <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ImageCounter.class);
		job.setJobName("Image Counter");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ImageCounterMapper.class);
		job.setReducerClass(ImageCounterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
