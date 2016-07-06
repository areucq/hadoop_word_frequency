import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class FrequencyReducer
		extends
		org.apache.hadoop.mapreduce.Reducer<Pair, IntWritable, Pair, FloatWritable> {
	HashMap<String, Integer> totalMap;
	HashMap<Pair, Integer> pairMap;

	public FrequencyReducer() {
		totalMap = new HashMap<String, Integer>();
		pairMap = new HashMap<Pair, Integer>();
	}

	@Override
	protected void reduce(
			Pair key,
			Iterable<IntWritable> ita,
			org.apache.hadoop.mapreduce.Reducer<Pair, IntWritable, Pair, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;

		Iterator<IntWritable> it = ita.iterator();
		while (it.hasNext()) {
			count += it.next().get();
		}

		pairMap.put(new Pair(new Text(key.getKey().toString()), new Text(key
				.getValue().toString())), new Integer(count));
		updateTotalMap(key, count);
	}

	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Reducer<Pair, IntWritable, Pair, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		for (Pair p : pairMap.keySet()) {
			Integer total = totalMap.get(p.getKey().toString());
			context.write(p, new FloatWritable(pairMap.get(p) * 1.0f / total));
		}

	}

	private void updateTotalMap(Pair pair, int count) {

		String leftKey = pair.getKey().toString();
		Object o = totalMap.get(leftKey);
		Integer total = 0;
		if (o != null) {
			total = (Integer) o;
		}
		total += count;
		totalMap.put(leftKey, total);
	}
}
