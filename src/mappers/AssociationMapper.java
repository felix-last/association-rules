package AssociationRules.mappers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// general dependencies
import java.util.List;
import java.io.IOException;
import java.lang.InterruptedException;


public class AssociationMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	/*
	*		INPUT FORMAT
	*						key		: lineidentifier
	*						value	: frequent itemset <{itemset}>\t<frequency>, e.g. 1;2;3	3
	*
	*		OUTPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. 1;2;3
	*						value	: frequency <frequency>, e.g. 3
	*
	*/


	public static enum Counters{
		PERMUTATIONS
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// value is a set of elements (parts[0]) and a frequency (parts[1])
		String[] parts = value.toString().split("\t");
		String[] raw = parts[0].split(";");

		Integer[] rawConverted = new Integer[raw.length];
		for (int i = 0; i < raw.length; i++){
			rawConverted[i] = Integer.parseInt(raw[i]);
		}

		// write all possible sortings of the input set to the context
		List<Integer[]> allPermutations = Utils.getAllPossiblePermutations(rawConverted);
		String concatenated = "";
		for (int i = 0; i < allPermutations.size(); i++) {
			String result = Utils.concatenateArray(allPermutations.get(i), ";");
			// key = A;B;N, value = count
			context.write(new Text(result), new DoubleWritable(Double.parseDouble(parts[1])));
			context.getCounter(Counters.PERMUTATIONS).increment(1);
		}
	}
}
