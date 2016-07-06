import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FrequencyReducer extends
		org.apache.hadoop.mapreduce.Reducer<Text, MapWritable, Text, Text> {
	HashMap<Text, MapWritable> totalMap;

	public FrequencyReducer() {
		totalMap = new HashMap<Text, MapWritable>();

	}

	@Override
	protected void reduce(
			Text key,
			Iterable<MapWritable> ita,
			org.apache.hadoop.mapreduce.Reducer<Text, MapWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		Iterator<MapWritable> it = ita.iterator();
		while (it.hasNext()) {
			wiseAdd(new Text(key.toString()), it.next());
		}

	}

	private void wiseAdd(Text key, MapWritable mw) {
		MapWritable existM = totalMap.get(key);
		if (existM == null) {
			existM = new MapWritable(mw);
		} else {

			for (Writable t : mw.keySet()) {
				IntWritable count = (IntWritable) mw.get(t);

				IntWritable existCount = (IntWritable) existM.get(t);
				if (existCount == null) {
					existCount = new IntWritable(count.get());
				} else {
					existCount = new IntWritable(existCount.get() + count.get());
				}

				existM.put(t, existCount);
			}
		}

		totalMap.put(key, existM);
	}

	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Reducer<Text, MapWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text t : totalMap.keySet()) {
			context.write(t, new Text(getFormatString(totalMap.get(t))));
		}

	}

	private String getFormatString(MapWritable mw) {
		int total = 0;

		for (Writable t : mw.keySet()) {
			IntWritable i = (IntWritable) mw.get(t);
			total += i.get();
		}

		StringBuilder sb = new StringBuilder();
		sb.append("[ ");

		for (Writable t : mw.keySet()) {
			sb.append(" ( ").append(((Text) t).toString()).append(" , ")
					.append(((IntWritable) mw.get(t)).get() * 1.0f / total)
					.append(" ),");

		}

		sb.append(" ]");

		return sb.toString();

	}

}
