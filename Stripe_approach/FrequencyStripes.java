import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FrequencyStripes {

	public static void main(String[] argv) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(FrequencyStripes.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(FrequencyMapper.class);

		job.setReducerClass(FrequencyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
				new Path(argv[0]));

		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(
				job, new Path(argv[1]));

		job.waitForCompletion(true);

	}
}
