import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FrequencyReducer extends
		org.apache.hadoop.mapreduce.Reducer<Pair, IntWritable, Text, Text> {
	HashMap<Text, MapWritable> totalMap;

	public FrequencyReducer() {
		totalMap = new HashMap<Text, MapWritable>();
	}

	@Override
	protected void reduce(
			Pair key,
			Iterable<IntWritable> ita,
			org.apache.hadoop.mapreduce.Reducer<Pair, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int count = 0;

		Iterator<IntWritable> it = ita.iterator();
		while (it.hasNext()) {
			count += it.next().get();
		}

		wiseAdd(new Pair(new Text(key.getKey().toString()), new Text(key
				.getValue().toString())), new IntWritable(count));
	}

	private void wiseAdd(Pair p, IntWritable count) {
		MapWritable existM = totalMap.get(p.getKey());
		if (existM == null) {
			existM = new MapWritable();
			existM.put(p.getValue(), count);
		} else {
			IntWritable curCount = (IntWritable) existM.get(p.getValue());
			if (curCount == null) {
				curCount = count;
			} else {
				curCount = new IntWritable(curCount.get() + count.get());
			}
			existM.put(p.getValue(), curCount);

		}

		totalMap.put(p.getKey(), existM);
	}

	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Reducer<Pair, IntWritable, Text, Text>.Context context)
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
