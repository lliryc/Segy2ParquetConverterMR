/**
 * Custom FileInputFormat implementation to provide SEGY-read within Hadoop
 * @author Kirill Chirkunov (https://github.com/lliryc)
 */
package com.chirkunov.mr.segy2parquet;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * FileInputFormat implementation for SEGY
 */
public class SEGYInputFormat extends FileInputFormat<TraceHeaderWritable, TraceWritable> {

	// segy (rev1) header size
	private static final int FILE_HEADER_SIZE = 3600;

	/**
	 * Returns TraceRecordReader for SEGY
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public RecordReader<TraceHeaderWritable, TraceWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TraceRecordReader();
	}

	/**
	 * Split SEGY-files on batches for further processing
	 * @throws IOException
	 * @throws InterruptedException
	 */
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
	// number of samples per trace - offset
	private static final int TRACES_SAMPLES_OFFSET = 3220;
	private static final int TRACES_SAMPLES_SIZE = 2;

	private static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN; // by default

	// trace header size in bytes
	public static final int TRACE_HEADER_SIZE = 240;
	// SEGY number format code - offset
	private static final int NUM_FORMAT_OFFSET = 3224;
	private static final int NUM_FORMAT_SIZE = 2;
	// partitions multiplier
	private static final int PARTITIONS_MULTIPLIER = 10;
	// traces per record - offset (not obligatory)
	private static final int TRACES_PER_RECORD_OFFSET = 3212;
	private static final int TRACES_PER_RECORD_SIZE = 2;
	// default number of data traces per record
	private static final short DEFAULT_TRACES_PER_RECORD = 2736;
	// Setting to store data samples number
	public static final String TRACE_SAMPLES_SETTING = "com.chirkunov.mr.segy2parquet.TRACE_SAMPLES";
	// size of one data sample in bytes
	public static final String TRACE_BYTE_PER_SAMPLE_SETTING = "com.chirkunov.mr.segy2parquet.TRACE_BYTES_PER_SAMPLE";
	// number format code
	public static final String TRACE_NUM_FMT_SETTING = "com.chirkunov.mr.segy2parquet.TRACE_NUM_FMT_SETTING";
	/**
	 * Compute a possible split length for the SEGY file, taking into account a trace size and a recommended minimal hdfs file size (>=64Mb)
	*/
	private int adjustSplitLength(Path file, JobContext job) throws IOException, IllegalArgumentException {
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		try {

			FSDataInputStream stream = fs.open(file);

			byte[] tracesPerRecordBytes = new byte[TRACES_PER_RECORD_SIZE];
			stream.seek(TRACES_PER_RECORD_OFFSET);
			stream.read(tracesPerRecordBytes, 0, TRACES_PER_RECORD_SIZE);
			short tracesPerRecord = ByteBuffer.wrap(tracesPerRecordBytes).order(BYTE_ORDER).getShort();
			if (tracesPerRecord == 0){
				tracesPerRecord = DEFAULT_TRACES_PER_RECORD;
			}

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
			return traceNBytes * tracesPerRecord * PARTITIONS_MULTIPLIER;
		} finally {
			fs.close();
		}
	}
}
