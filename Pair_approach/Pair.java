import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Serializable, WritableComparable<Pair> {

	private static long serialVersionUID = 18823482348L;

	public Pair() {
		key = new Text();
		value = new Text();
	}

	public Pair(Text key, Text value) {
		this.key = key;
		this.value = value;
	}

	Text key;
	Text value;

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	@Override
	public String toString() {

		return "(" + key.toString() + ", " + value.toString() + ")";
	}

	@Override
	public int hashCode() {

		return (key.toString() + value.toString()).hashCode();
	}

	@Override
	public int compareTo(Pair o) {
		int result = key.compareTo(o.getKey());
		if (result != 0) {
			return result;
		}
		return value.compareTo(o.getValue());
	}

	@Override
	public void readFields(DataInput reader) throws IOException {
		key.readFields(reader);
		value.readFields(reader);

	}

	@Override
	public void write(DataOutput writer) throws IOException {
		key.write(writer);
		value.write(writer);

	}
}
