package com.chirkunov.mr.segy2parquet;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TraceRecordReader extends RecordReader<TraceHeaderWritable, TraceWritable>{
	private FSDataInputStream inputStream = null;
	private long start;
    private long end;
    private long pos;
    private TraceHeaderWritable key = new TraceHeaderWritable() ;
    private TraceWritable value = new TraceWritable();
    protected Configuration conf;
    private boolean processed = false;
    private int nSamples;
    private int bytesPerSample;
    private int nFmt;
    
	@Override
	public void close() throws IOException {
			inputStream.close();		
	}
	@Override
	public TraceHeaderWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	@Override
	public TraceWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ((processed == true)? 1.0f : 0.0f);
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
		this.start = fileSplit.getStart();  
        this.end = this.start + fileSplit.getLength();
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf); 
		this.inputStream = fs.open(path);  
		inputStream.seek(this.start);  
		this.pos = this.start;
		this.nSamples =  conf.getInt(SEGYInputFormat.TRACE_SAMPLES_SETTING, 3000);
		this.bytesPerSample =  conf.getInt(SEGYInputFormat.TRACE_BYTE_PER_SAMPLE_SETTING, 4);
		this.nFmt =  conf.getInt(SEGYInputFormat.TRACE_NUM_FMT_SETTING, 4);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (this.pos < this.end) {
			int traceSize = SEGYInputFormat.TRACE_HEADER_SIZE + this.nSamples * this.bytesPerSample;
			byte[] traceBytes = new byte[traceSize];
			inputStream.read(traceBytes);
			value.set(traceBytes, bytesPerSample, nFmt, nSamples);
			key.set(value.getTraceHeader());
			this.pos = inputStream.getPos();
			return true;
		} else {
			processed = true;
			return false;
		}
	}
}
