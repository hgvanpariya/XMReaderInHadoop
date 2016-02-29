package my.hari;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyExecutor extends Configured implements Tool  {

	
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyExecutor(), args);
        System.exit(res);
		
		
	}

	@Override
	public int run(String[] args) throws Exception {
//		Configuration configuration = new Configuration();
		Configuration configuration  = super.getConf();
		configuration.set(XmlInputFormat.START_TAG_KEY, "<class>");
		configuration.set(XmlInputFormat.END_TAG_KEY, "</class>");
		try {
			Job j = new Job(configuration, "MyWordCounteJob");

			j.setJarByClass(MyExecutor.class);
			j.setMapperClass(MyMapper.class);
			j.setReducerClass(MyReducer.class);

			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(Text.class);

			j.setInputFormatClass(XmlInputFormat.class);
			j.setOutputFormatClass(TextOutputFormat.class);

			Path outputPath = new Path(args[1]);

			FileInputFormat.addInputPath(j, new Path(args[0]));
			FileOutputFormat.setOutputPath(j, new Path(args[1]));

			outputPath.getFileSystem(configuration).delete(outputPath);
			
//			System.exit(j.waitForCompletion(true) ? 0 : 1);
			
			if (j.waitForCompletion(true)) {
	            return 0;
	        }
	        return 1;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

}
