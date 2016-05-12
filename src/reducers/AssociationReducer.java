package AssociationRules.reducers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// general dependencies
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;

public class AssociationReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. 1;2;3
	*						value	: frequency <frequency>, e.g. 5
	*
	*		OUTPUT FORMAT
	*						key		: Rule <{independent items}==>{dependent items}>, e.g. A;B==>C
	*						value	: frequency <frequency>, e.g. 5 
	*
	*/


	public static enum Counters{
		RULES
	}

	// by using this map the output of the reducer can be sorted by frequency
	private Map<Text, IntWritable> resultMap = new HashMap<>();

	// blacklist ensures that rules aren't outputted multiple times
	private List<String[]> blackList = new ArrayList();

	// translate item name to integer id
	// public static Map<String, Integer> itemKey = new HashMap<>(); // not needed
	public static Map<Integer, String> keyItem = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// read the mapping of keys to item names
		try{
			String path = context.getConfiguration().get("TMP_FILE_PATH");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			keyItem = (HashMap) Utils.deserializeObject(fs, path+"key-itemMap.ser");
			System.out.println("Successfully deserialized item key mapping.");
		} catch(Exception e){
			System.err.println("Failed deserialization of item key mapping: "+e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// values should only have 1 element

		int sum = 0;
		int count = 0;
		for (IntWritable val : values){
			sum += val.get();
			count++;
		}

		String[] raw = key.toString().split(";");

		int maxDegree = raw.length-1;

		for (int deg = 1; deg <= maxDegree; deg++){
			String outputKey = "";
			if (raw.length-1 < deg){
				break;
			}
			for (int i = 0; i<raw.length; i++){
				outputKey += raw[i];
				if (i == deg-1){
					outputKey += "==>";
				} else if (i<raw.length-1) {
					outputKey += ";";
				}
			}

			// if not all possible degrees of rules are used, duplicates will be produced 
			// and should not be written to map (in order to prevent integrity issues)
			if (resultMap.get(new Text(outputKey)) == null){
				// check if for the independent elements a similar rule already exists
				// to prevent this:
				// A==>C;D	3
				// A==>D;C	3				
				if (!isBlacklisted(outputKey)){
					resultMap.put(new Text(outputKey), new IntWritable(sum));
					addToBlacklist(outputKey);
				}
			}
		}
	}


	public void cleanup(Context context) throws IOException, InterruptedException{
		// sort keys by frequency and writes to context
        Map<Text, IntWritable> sortedResultMap = Utils.sortMapByValues(resultMap);

        for (Text key : sortedResultMap.keySet()) {
        	context.getCounter(Counters.RULES).increment(1);
        	// transform key into actual item names
        	String[] comps = key.toString().split("==>");
        	for (int i = 0; i < comps.length; i++){
        		String[] ks = comps[i].split(";");
        		comps[i] = "";
        		for (int k = 0; k < ks.length; k++){
        			comps[i] += keyItem.get(Integer.parseInt(ks[k]));
        			if (k < ks.length-1) comps[i] += ";";
        		}
        	}
        	Text out = new Text(""+comps[0]+"==>"+comps[1]);
    		context.write(out, sortedResultMap.get(key));
        }

        System.out.println("Cleanup: Number of distinct rules = " + context.getCounter(Counters.RULES).getValue());

	}

	private boolean isBlacklisted(String rule){
		boolean blacklisted = false;
		String[] inputComponents = rule.split("==>");
		for (int i = 0; i<2; i++){
			String[] tmp = inputComponents[i].split(";");
			Arrays.sort(tmp);
			inputComponents[i] = Arrays.toString(tmp);
			tmp = null;
		}

		for (int i = 0; i < blackList.size(); i++){
			String[] bLComponents = blackList.get(i);
			if (bLComponents[0].equals(inputComponents[0]) && bLComponents[1].equals(inputComponents[1])) {
				blacklisted = true;
			}
		}
		return blacklisted;
	}

	private void addToBlacklist(String rule){
		String[] ruleComponents = rule.split("==>");
		for (int i = 0; i<2; i++){
			String[] tmp = ruleComponents[i].split(";");
			Arrays.sort(tmp);
			ruleComponents[i] = Arrays.toString(tmp);
			tmp = null;
		}
		String[] blacklistItem = { ruleComponents[0], ruleComponents[1] };
		blackList.add(blacklistItem);
	}
}
