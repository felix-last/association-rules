package AssociationRules.reducers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.DoubleWritable;
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

public class AssociationReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	/*
	*		INPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. 1;2;3
	*						value	: frequency <frequency>, e.g. 5
	*
	*		OUTPUT FORMAT
	*						key		: Rule <{independent items}==>{dependent items}>, e.g. A;B==>C
	*						value	: confidence , e.g. 0.7
	*
	*/


	public static enum Counters{
		RULES
	}

	// by using this map the output of the reducer can be sorted by frequency
	private Map<Text, DoubleWritable> resultMap = new HashMap<>();
	private Map<Text, DoubleWritable> frequencies = new HashMap<>();

	// blacklist ensures that rules aren't outputted multiple times
	private List<String[]> blackList = new ArrayList();

	// translate item name to integer id
	// public static Map<String, Integer> itemKey = new HashMap<>(); // not needed
	private static Map<Integer, String> keyItem = new HashMap<>();

	// helper files location
	private static String basePath = "";

	// how to separate rule components: e.g. A;B==>C
	private static String componentDelimiter = "==>";
	private static String itemSeparator = ";";

	// confidence threshold
	private static Double confidenceThreshold = 0.5;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// read configuration
		basePath = context.getConfiguration().get("TMP_FILE_PATH");
		componentDelimiter = context.getConfiguration().get("RULE_COMPONENT_DELIMITER");
		itemSeparator = context.getConfiguration().get("RULE_ITEM_SEPARATOR");
		confidenceThreshold = Double.parseDouble(context.getConfiguration().get("CONFIDENCE_THRESHOLD"));

		// read the mapping of keys to item names
		try{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			keyItem = (HashMap) Utils.deserializeObject(fs, basePath+"key-itemMap.ser");
			System.out.println("Successfully deserialized item key mapping.");
		} catch(Exception e){
			System.err.println("Failed deserialization of item key mapping: "+e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// values should only have 1 element

		Double sum = 0.0;
		int count = 0;
		for (DoubleWritable val : values){
			sum += val.get();
			count++;
		}

		// save to frequencies for confidence calculation
		frequencies.put(new Text(key), new DoubleWritable(sum));

		String[] raw = key.toString().split(";");

		// ensure 1-tupels are ignored
		if (raw.length > 1){
			int maxDegree = raw.length-1;

			for (int deg = 1; deg <= maxDegree; deg++){
				String outputKey = "";
				if (raw.length-1 < deg){
					break;
				}
				for (int i = 0; i<raw.length; i++){
					outputKey += raw[i];
					if (i == deg-1){
						outputKey += componentDelimiter;
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
						resultMap.put(new Text(outputKey), new DoubleWritable(sum));
						addToBlacklist(outputKey);
					}
				}
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Map<Text, DoubleWritable> intermediaryResultMap = new HashMap<>();
		// prepare results for output
        for (Text key : resultMap.keySet()) {
        	context.getCounter(Counters.RULES).increment(1);
        	
        	String[] comps = key.toString().split(componentDelimiter);
        	
        	// calculate confidence
        	Double confidence = resultMap.get(key).get();
			try{
				Double indepSupport = frequencies.get(new Text(comps[0])).get();
        		confidence = (confidence/indepSupport);
			} catch(Exception e){
				System.out.println("Failed calculating confidence: "+e.getMessage());
			}

        	// transform key into actual item names
        	for (int i = 0; i < comps.length; i++){
        		String[] ks = comps[i].split(";");
        		comps[i] = "{";
        		for (int k = 0; k < ks.length; k++){
        			comps[i] += keyItem.get(Integer.parseInt(ks[k]));
        			if (k < ks.length-1) comps[i] += itemSeparator;
        		}
        		comps[i] += "}";
        	}
        	Text out = new Text(""+comps[0]+componentDelimiter+comps[1]);
        	intermediaryResultMap.put(out, new DoubleWritable(confidence));
        }
        resultMap = null;

        // output sorted list
		Map<Text, DoubleWritable> sortedResultMap = Utils.sortMapByValues(intermediaryResultMap);
		intermediaryResultMap = null;
    	for (Text key : sortedResultMap.keySet()){
    		DoubleWritable val = sortedResultMap.get(key);
    		if (val.get() > confidenceThreshold){
    			context.write(key, val);
    		}
    	}

        System.out.println("Cleanup: Number of distinct rules = " + context.getCounter(Counters.RULES).getValue());

	}

	private boolean isBlacklisted(String rule){
		boolean blacklisted = false;
		String[] inputComponents = rule.split(componentDelimiter);
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
		String[] ruleComponents = rule.split(componentDelimiter);
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
