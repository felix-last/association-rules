package AssociationRules.mappers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// general dependencies
import java.util.List;
import java.io.IOException;
import java.lang.InterruptedException;


public class AssociationMapper extends Mapper<Object, Text, Text, IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: lineidentifier
	*						value	: frequent itemset <{itemset}>\t<frequency>, e.g. A;B;C	3
	*
	*		OUTPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. A;B;C
	*						value	: frequency <frequency>, e.g. 3
	*
	*/


	public static enum Counters{
		PERMUTATIONS
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// value is a set of elements (parts[0]) and a frequency (parts[1])
		String[] parts = value.toString().split("\t");
		String[] raw = parts[0].split(";");

		// write all possible sortings of the input set to the context
		List<String[]> allPermutations = Utils.getAllPossiblePermutations(raw);
		String concatenated = "";
		for (int i = 0; i < allPermutations.size(); i++) {
			String result = Utils.concatenateArray(allPermutations.get(i), ";");
			// key = A;B;N, value = count
			context.write(new Text(result), new IntWritable(Integer.parseInt(parts[1])));
			context.getCounter(Counters.PERMUTATIONS).increment(1);
		}

	}
}
