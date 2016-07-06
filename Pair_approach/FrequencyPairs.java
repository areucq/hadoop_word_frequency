import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FrequencyPairs {

	public static void main(String[] argv) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(FrequencyPairs.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(FrequencyMapper.class);

		job.setReducerClass(FrequencyReducer.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
				new Path(argv[0]));

		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
				job, new Path(argv[1]));

		job.waitForCompletion(true);

	}
}
