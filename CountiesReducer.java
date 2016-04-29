package countiesproject;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import mrtools.CountMap;
import mrtools.NBest;

public class CountiesReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	NBest<String> NB = new NBest<String>(3);
	HashMap<String, Integer> ratio = new HashMap<String, Integer>();

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		ratio.clear();
		while (values.hasNext()) {
			String value = values.next().toString();
			String[] parts = value.split("\\|");

			try {

				ratio.put(parts[0].trim() + "\tCrime \t" + parts[2].trim(),
						(int) Double.parseDouble(parts[1])*100);

			} catch (NumberFormatException nfe) {

			}
		}

		NB.clear();
		NB.putMap(ratio);
		output.collect(key, new Text(NB.bestEntryCountTSV("\tPop\t")));
	}
}