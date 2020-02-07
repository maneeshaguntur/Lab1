package ques4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Summary implements Writable {
	IntWritable requests;
	IntWritable downloadSize;

	Summary() {
		requests = new IntWritable();
		downloadSize = new IntWritable();
	}

	Summary(IntWritable requests, IntWritable downloadSize) {
		this.requests = requests;
		this.downloadSize = downloadSize;
	}

	public IntWritable getDownloadSize() {
		return downloadSize;
	}

	public void setDownloadSize(IntWritable downloadSize) {
		this.downloadSize = downloadSize;
	}

	public IntWritable getRequests() {
		return requests;
	}

	public void setRequests(IntWritable requests) {
		this.requests = requests;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		requests.write(dataOutput);
		downloadSize.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		requests.readFields(dataInput);
		downloadSize.readFields(dataInput);
	}

	@Override
	public String toString() {
		return requests + "," + downloadSize;
	}
}
