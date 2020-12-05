/**
 * Custom implementation of RecordReader for extracting trace records from SEGY file
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
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

/**
 * Custom implementation of RecordReader<TraceHeaderWritable, TraceWritable> to read SEGY traces
 */
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

	/**
	 * Close read session
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
			inputStream.close();		
	}

	/**
	 * Returns key for current record
	 * @return
	 */
	@Override
	public TraceHeaderWritable getCurrentKey()  {
		return key;
	}

	/**
	 * Returns value for current record
	 * @return
	 */
	@Override
	public TraceWritable getCurrentValue() {
		return value;
	}

	/**
	 * True if reader has already finished reading SEGY, False otherwise
	 * @return
	 */
	@Override
	public float getProgress()  {
		return ((processed == true)? 1.0f : 0.0f);
	}

	/**
	 * Initialize TraceRecordReader
	 * @param split set of split ranges
	 * @param context Task attempt context
	 * @throws IOException
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
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

	/**
	 * Read the next value from SEGY split partition
	 * @return
	 * @throws IOException
	 */
	@Override
	public boolean nextKeyValue() throws IOException {

		if (this.pos < this.end) {
			int traceSize = SEGYInputFormat.TRACE_HEADER_SIZE + this.nSamples * this.bytesPerSample;
			byte[] traceBytes = new byte[traceSize];
			inputStream.read(traceBytes);
			value.set(traceBytes, nFmt, nSamples);
			key.set(value.getTraceHeader());
			this.pos = inputStream.getPos();
			return true;
		} else {
			processed = true;
			return false;
		}
	}
}
