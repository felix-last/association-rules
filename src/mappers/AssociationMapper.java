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

/**
 * AssociationMapper is the mapper used for extraction of the association rules.
 * The class handles the input preparation of the map reduce job by parsing
 * the input files into key, value pairs and generating all permutations.
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class AssociationMapper extends Mapper<Object, Text, Text, IntWritable> {

	/**
     * Enumerator keeping count of the total key, value pairs that have been written to the context
	 */
	public static enum Counters{
		PERMUTATIONS
	}
	
	/**
     * Parses an input value into a key, value pair and generates all permutations. The method will 
     * take the input key and split it by a tab character into a key and a value. The key is an
     * itemset and the value the count for the itemset. The mapper will then write all permutations
     * (sortings) of the itemset to the context.<p>
     * INPUT FORMAT<p>
	 * key	: lineidentifier<br>
	 * value: frequent itemset and count, e.g. 1;2;3tab3<p>
	 *
	 * OUTPUT FORMAT<p>
	 * key	: Itemset permutation, e.g. 1;2;3<br>
	 * value: frequency of itemset, e.g. 5<p>
	 *
     * @param key					a input file lineidentifier
     * @param value					the concatenated itemset-count pair
     * @param context 				the context of the map reduce job
     *
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
     * @see 						AssociationRules.util.Utils#getAllPossiblePermutations(Integer[])
     */
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
			context.write(new Text(result), new IntWritable(Integer.parseInt(parts[1])));
			context.getCounter(Counters.PERMUTATIONS).increment(1);
		}
	}
}
