package countiesproject;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountiesJob extends Configured implements Tool{

  public static void main (String[] args) throws Exception{
	  int result = ToolRunner.run(new Configuration(), new  CountiesJob(), args);
	  System.exit(result);
  }

	@Override
	public int run(String[] args) throws Exception {

		Configuration C = getConf();
		JobConf conf = new JobConf(C, CountiesJob.class);
		try {
			FileSystem fs = FileSystem.get(conf);
			Path Out = new Path(args[1]);
			if (fs.exists(Out)) {
				fs.delete(Out, true);
				Log L = LogFactory.getLog(this.getClass());
				L.info("Removed old output directory: " + Out.toString());

			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		conf.setJobName("GovPurchases");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(CountiesMapper.class);
		conf.setReducerClass(CountiesReducer.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);

		File file = new File("/home/student/output/part-00000");
		Scanner inFile = new Scanner(file);
		PrintWriter oFile = new PrintWriter(new File(
				"/home/student/output/Finaloutput.txt"));
		int count1=0,count2=0,count3=0;
		while (inFile.hasNext()) {

			String[] parts = inFile.nextLine().split("\t");
			try {

				if ((Double.parseDouble(parts[3]) > Double
						.parseDouble(parts[8]))
						&& (Double.parseDouble(parts[8]) > Double
								.parseDouble(parts[13]))) {
					oFile.println(parts[0] + "\t" + parts[3] + "\t" + parts[8]
							+ "\t" + parts[13] + "\t" + "Group[1]");
					count1++;
				} else if ((Double.parseDouble(parts[3]) < Double
						.parseDouble(parts[8]))
						&& (Double.parseDouble(parts[8]) < Double
								.parseDouble(parts[13]))) {
					oFile.println(parts[0] + "\t" + parts[3] + "\t" + parts[8]
							+ "\t" + parts[13] + "\t" + "Group[2]");
					count2++;
				} else {
					oFile.println(parts[0] + "\t" + parts[3] + "\t" + parts[8]
							+ "\t" + parts[13] + "\t" + "Group[3]");
					count3++;
				}
			} catch (IndexOutOfBoundsException e) {

			}
		}
		oFile.println("Group[1]\t"+count1+"\nGroup[2]\t"+count2+"\nGroup[3]\t"+count3);

		inFile.close();
		oFile.close();

		return 0;

	}
}
