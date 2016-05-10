package AssociationRules.mappers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

// general dependencies
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import java.lang.InterruptedException;


public class CandidateMapper extends Mapper<Object, Text, Text, IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: lineidentifier
	*						value	: basket <{basket items}>, e.g. A.B.C.D
	*
	*		OUTPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. A;B;C
	*						value	: 1	(count is always 1 ;) ) 
	*
	*/

	public static enum Counters{
		POWERSETS,
		INPUTLINES
	}

	private final static IntWritable one = new IntWritable(1);
	private Set<String> inputSet = new HashSet();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// convert input into array
		String[] raw = value.toString().split("\\.");

		// convert to set (ensures that there are no duplicates)
		inputSet = new HashSet(Arrays.asList(raw));

		// create every possible subset (consisting of minNumElements)
		int minNumElements = Integer.parseInt(context.getConfiguration().get("MIN_NUMBER_ELEMENTS"));
		HashSet<List<String>> powerset = Utils.powerSet(inputSet, minNumElements);

		// increment counters
		context.getCounter(Counters.POWERSETS).increment(powerset.size());
		context.getCounter(Counters.INPUTLINES).increment(1);

		// write each concatenated set to context with counter 1
		Iterator<List<String>> it = powerset.iterator();
		while (it.hasNext()){
			List<String> subset = it.next();
			String result = Utils.concatenateArray(subset.toArray(new String[0]), ";");
			context.write(new Text(result), one);
		}

	}

	public void cleanup(Context context){
		System.out.println("Cleanup: Powerset Counter = " + context.getCounter(Counters.POWERSETS).getValue());
		System.out.println("Cleanup: Inputlines Counter = " + context.getCounter(Counters.INPUTLINES).getValue());
	}
}
