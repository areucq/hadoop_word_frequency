import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class FrequencyMapper
		extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, MapWritable> {

	HashMap<String, HashMap<String, Integer>> stripMap;

	public FrequencyMapper() {
		stripMap = new HashMap<String, HashMap<String, Integer>>();
	}

	@Override
	protected void map(
			LongWritable docid,
			Text doc,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, MapWritable>.Context context)
			throws IOException, InterruptedException {

		String txt = doc.toString().trim();
		// System.out.println("map method called ; txt " + txt);

		String[] words = txt.split(" ");

		for (int i = 0; i < words.length; i++) {
			getNieghbour(i, words);

		}

	}

	@Override
	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, MapWritable>.Context context)
			throws IOException, InterruptedException {
		for (String key : stripMap.keySet()) {
			HashMap<String, Integer> m = stripMap.get(key);

			MapWritable mw = new MapWritable();
			for (String k : m.keySet()) {
				mw.put(new Text(k), new IntWritable(m.get(k).intValue()));
			}
			context.write(new Text(key), mw);
		}

	}

	private void getNieghbour(int index, String[] src) {
		String key = src[index];

		HashMap<String, Integer> map = stripMap.get(key);
		if (map == null)
			map = new HashMap<String, Integer>();

		if (index == src.length)
			return;

		index++;

		while (index < src.length) {
			String nextWord = src[index];
			if (nextWord.equals(key))
				break;

			Integer count = map.get(nextWord);
			if (count == null) {
				count = new Integer(1);
			} else {
				count += 1;
			}

			map.put(nextWord, count);
			index++;
		}

		stripMap.put(key, map);
	}

}
