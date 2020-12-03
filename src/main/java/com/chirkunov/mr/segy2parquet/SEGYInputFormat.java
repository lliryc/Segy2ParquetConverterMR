package com.chirkunov.mr.segy2parquet;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class SEGYInputFormat extends FileInputFormat<TraceHeaderWritable, TraceWritable> {

	private static final int FILE_HEADER_SIZE = 3600;

	@Override
	public RecordReader<TraceHeaderWritable, TraceWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TraceRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files) {
			Path path = file.getPath();

			long length = file.getLen();
			if (length != 0) {
				BlockLocation[] blkLocations;
				if (file instanceof LocatedFileStatus) {
					blkLocations = ((LocatedFileStatus) file).getBlockLocations();
				} else {
					FileSystem fs = path.getFileSystem(job.getConfiguration());
					blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				if (isSplitable(job, path)) {
					long splitSize = adjustSplitLength(file.getPath(), job);
					long bytesRemaining = length - FILE_HEADER_SIZE;
					while (((double) bytesRemaining) / splitSize > 1) {
						int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
						System.out.println(length - bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining, splitSize,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
						bytesRemaining -= splitSize;
					}
					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
					}
				} else {
					splits.add(makeSplit(path, FILE_HEADER_SIZE, length - FILE_HEADER_SIZE, blkLocations[0].getHosts(),
							blkLocations[0].getCachedHosts()));
				}
			} else {
				splits.add(makeSplit(path, 0, length, new String[0]));
			}
		}
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		return splits;
	}

	private static final int TRACES_SAMPLES_OFFSET = 3220;
	private static final int TRACES_SAMPLES_SIZE = 2;
	private static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN; // by default
	public static final int TRACE_HEADER_SIZE = 240;
	private static final int NUM_FORMAT_OFFSET = 3224;
	private static final int NUM_FORMAT_SIZE = 2;
	private static final int TRACES_MULTIPLIER = 1000;

	public static final String TRACE_SAMPLES_SETTING = "com.chirkunov.mr.segy2parquet.TRACE_SAMPLES";
	public static final String TRACE_BYTE_PER_SAMPLE_SETTING = "com.chirkunov.mr.segy2parquet.TRACE_BYTES_PER_SAMPLE";
	public static final String TRACE_NUM_FMT_SETTING = "com.chirkunov.mr.segy2parquet.TRACE_NUM_FMT_SETTING";

	private int adjustSplitLength(Path file, JobContext job) throws IOException, IllegalArgumentException {
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		try {
			FSDataInputStream stream = fs.open(file);

			byte[] nSamplesBytes = new byte[TRACES_SAMPLES_SIZE];
			stream.seek(TRACES_SAMPLES_OFFSET);
			stream.read(nSamplesBytes, 0, TRACES_SAMPLES_SIZE);
			short nSamples = ByteBuffer.wrap(nSamplesBytes).order(BYTE_ORDER).getShort();
			job.getConfiguration().setInt(TRACE_SAMPLES_SETTING, nSamples);

			byte[] numFormatBytes = new byte[NUM_FORMAT_SIZE];
			stream.seek(NUM_FORMAT_OFFSET);
			stream.read(numFormatBytes, 0, NUM_FORMAT_SIZE);
			short numFormat = ByteBuffer.wrap(numFormatBytes).order(BYTE_ORDER).getShort();
			int nBytes = NumFormatUtil.numBytesByFormat(numFormat);
			job.getConfiguration().setInt(TRACE_BYTE_PER_SAMPLE_SETTING, nBytes);
			job.getConfiguration().setInt(TRACE_NUM_FMT_SETTING, numFormat);

			int traceNBytes = TRACE_HEADER_SIZE + nSamples * nBytes;
			return traceNBytes * TRACES_MULTIPLIER;
		} finally {
			fs.close();
		}
	}
}
