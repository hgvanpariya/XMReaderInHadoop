package my.hari;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 */

/**
 * @author edureka
 * 
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {

	// protected void reduce(
	// Text key,
	// java.lang.Iterable<IntWritable> arg1,
	// org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text,
	// IntWritable>.
	// Context arg2)
	// throws java.io.IOException, InterruptedException {
	//
	// for (IntWritable intWritable : arg1) {
	//
	// }
	// };
	//

	protected void reduce(
			Text key,
			java.lang.Iterable<Text> value,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		StringBuffer size = new StringBuffer();

		for (Text intWritable : value) {
			size.append(intWritable + " ");

		}
		context.write(key, new Text(size.toString()));
	};
}
