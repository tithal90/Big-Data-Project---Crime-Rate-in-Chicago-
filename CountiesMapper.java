package countiesproject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;

import mrtools.number;
import mrtools.ColumnReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


public class CountiesMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private ColumnReader Reader = null;

	@Override
	public void configure(JobConf job) {

		int[] Columns = { 4, 2, 3, 5, 20, 43, 276, 6, 3091, 8 };
		Reader = new ColumnReader(Columns);
		Reader.setColName(5, "StateName");
		Reader.setColName(6, "CountyName");
		Reader.setColName(4, "FIBS");
		Reader.setColName(8, "PopulationGr");
		Reader.setColName(10, "CrimeRate");
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		Reader.reset(line);
		if (Reader.match()) {
			String k = Reader.get("StateName");
			if (k == null) {
				reporter.incrCounter("Input Read Errors", "Unknown state code",
						1);
				return;
			}
			try {
				Reader.get("PopulationGr");
			} catch (NumberFormatException e) {
				reporter.incrCounter("Input Read Errors",
						"Invalid Population Growth", 1);
				return;
			}
			try {
				Reader.get("CrimeRate");
			} catch (NumberFormatException e) {
				reporter.incrCounter("Input Read Errors",
						"Invalid PopCrime rate", 1);
				return;
			}
			if (Reader.get("PopulationGr").trim().isEmpty()) {
				return;
			}
			if (Reader.get("CrimeRate").trim().isEmpty()) {
				return;
			}
			String v = Reader.get("CountyName") + "|"
					+ Reader.get("PopulationGr") + "|"
					+ Reader.get("CrimeRate");
			output.collect(new Text(k), new Text(v));
		}
	}

	@Override
	public void close() {

	}
}