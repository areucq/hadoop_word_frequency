import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class FrequencyMapper
		extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Pair, IntWritable> {

	HashMap<Pair, Integer> map;

	public FrequencyMapper() {
		map = new HashMap<Pair, Integer>();
	}

	@Override
	protected void map(
			LongWritable docid,
			Text doc,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String txt = doc.toString().trim();

		String[] words = txt.split(" ");

		for (int i = 0; i < words.length; i++) {
			List<Pair> neighbours = getNieghbour(i, words);
			updatePairToMap(neighbours);
		}

	}

	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		for (Pair p : map.keySet()) {
			context.write(p, new IntWritable(map.get(p)));
		}

	}

	private void updatePairToMap(List<Pair> pairs) {
		for (Pair p : pairs) {
			Object o = map.get(p);
			Integer t = 0;
			if (o != null) {
				t = (Integer) o;

			}
			t += 1;

			map.put(p, t);
		}
	}

	private List<Pair> getNieghbour(int index, String[] src) {

		List<Pair> neighbours = new ArrayList<Pair>();
		if (index == src.length)
			return neighbours;

		String key = src[index];
		index++;

		while (index < src.length) {
			String nextWord = src[index];
			if (nextWord.equals(key))
				break;

			neighbours.add(new Pair(new Text(key), new Text(nextWord)));
			index++;
		}

		return neighbours;
	}

}
