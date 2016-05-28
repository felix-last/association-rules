package AssociationRules.reducers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
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

/**
 * AssociationReducer handles the concatenation of itemsets into rules and the calculation of their
 * confidence. Each input set will be split into all possible rules, meaning that e.g. from 1;2;3
 * the rules 1-2;3 and 1;2-3 will be created (the sorting will not be changed, it is 
 * the @link AssociationRules.mappers.AssociationMapper job to produce all possible permutations). To calculate the confidence a frequencie map is maintained
 * that is used for getting the count of the antedecent of the rule.<p>
 * It relies on the following job configurations:
 * <ul>
 * <li>RULE_COMPONENT_DELIMITER: character sequence that splits rules</li>
 * <li>RULE_ITEM_SEPARATOR: character sequence that splits items in the antedecent and consequent of a rule</li>
 * <li>TMP_FILE_PATH: the file system location, where the intermediary files are to be found</li>
 * <li>CONFIDENCE_THRESHOLD: minimum confidence that a rule has to reach</li>
 * </ul>
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class AssociationReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

	/**
     * Enumerator keeping count of the total extracted rules.
	 */
	public static enum Counters{
		RULES
	}

	/**
     * Map to keep the rules that have been generated to calculate their confidence.
     */
	private Map<Text, DoubleWritable> resultMap = new HashMap<>();
	
	/**
     * Map to keep the frequencies of all itemsets. Used to find the support of the antedecent of the rule.
     */
	private Map<Text, DoubleWritable> frequencies = new HashMap<>();

	/**
     * Blacklist to not output duplicate rules. The blacklist will prevent that e.g. 1-2;3 and 1-3;2 are 
     * seen as two distinct rules.
     */
	private List<String[]> blackList = new ArrayList();

	/**
     * Translate integer value to item name, loaded from file if possible.
     */
	private static Map<Integer, String> keyItem = new HashMap<>();

	/**
     * Location of the helperfiles, loaded from file if possible.
     */
	private static String basePath = "";

	/**
     * Character sequence that separates antedecent from consequent, updated from configuration.
     */
	private static String componentDelimiter = "==>";
	
	/**
     * Character sequence that separates items in the antedecent and consequent, updated from configuration.
     */
	private static String itemSeparator = ";";
	
	/**
     * Confidence threshold applied to the extracted rules, updated from configuration.
     */
	private static Double confidenceThreshold = 0.5;

    /**
     * Preparing the reducer for execution by loading necessary files and reading configuration.
     * The mapping from keys to item names is loaded here and the configuration is read to update
     * the members componentDelimiter, itemSeparator, confidenceThreshold and basePath.
     * @param context   			context of mapper
     * 
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
     *
     * @see 		AssociationRules.util.Utils#deserializeObject(FileSystem, String)
     *
     */
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

	/**
     * Reduce function to sum up the counts for a key and create the different rules from the itemset.
     * The method will firstly sum up if necessary the values received and then put the itemset with its count
     * into the frequencies map. The reducer will then generate all rules by splitting the itemset at each position.
     * The resulting rules will be put into the resultMap for later calculation of the confidence, if the blacklist 
     * is not violated.<p>
     * INPUT FORMAT<p>
	 * key	: Itemset, e.g. 1;2;3<br>
	 * value: 2342 <p>
	 *
	 * OUTPUT FORMAT<p>
	 * key	: Rule, e.g. A;B-C<br>
	 * value: confidence of rule, e.g. 0.57 <p>
	 *
     * @param key					itemset key, e.g. 1;2;3
     * @param values				the iterable counts for the key
     * @param context 				the context of the map reduce job
     *
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
     */ 
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int sum = 0;
		int count = 0;
		for (IntWritable val : values){
			sum += val.get();
			count++;
		}

		// save to frequencies for confidence calculation
		frequencies.put(new Text(key), new DoubleWritable((double)sum));

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
						resultMap.put(new Text(outputKey), new DoubleWritable((double)sum));
						addToBlacklist(outputKey);
					}
				}
			}
		}
	}

    /**
     * Finishing the rules extraction by calculating confidences and converting the rules into readable format. 
     * The cleanup will firstly loop over the resultMap and calculate the confidences, if a rule exceeds the confidence
     * threshold it will be written to the intermediaryResultMap. The intermediaryResultMap will 
     * then be sorted by values. The last step is to convert all rules on the intermediaryResultMap back to their item names
     * and write them to the context with the calculated confidence.
     *
     * @param context   			context of mapper
     * 
     * 
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
	 *
     * @see 						AssociationRules.util.Utils#sortMapByValues(Map)
     *
     */
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
			if (confidence >= confidenceThreshold){
        		intermediaryResultMap.put(new Text(key.toString()), new DoubleWritable(confidence));
			}        
        }
        resultMap = null;

        // output sorted list
		Map<Text, DoubleWritable> sortedResultMap = Utils.sortMapByValues(intermediaryResultMap);
		intermediaryResultMap = null;
    	for (Text key : sortedResultMap.keySet()){
    		DoubleWritable val = sortedResultMap.get(key);
        	// transform key into actual item names
        	String[] comps = key.toString().split(componentDelimiter);
        	for (int i = 0; i < comps.length; i++){
        		String[] ks = comps[i].split(";");
        		comps[i] = "{";
        		for (int k = 0; k < ks.length; k++){
        			comps[i] += keyItem.get(Integer.parseInt(ks[k]));
        			if (k < ks.length-1) comps[i] += itemSeparator;
        		}
        		comps[i] += "}";
        	}
			context.write(new Text(""+comps[0]+componentDelimiter+comps[1]), val);
    	}

        System.out.println("Cleanup: Number of distinct rules = " + context.getCounter(Counters.RULES).getValue());

	}


    /**
     * Check whether a rule is on the blacklist or not. Checking if a similar rule is already accepted, to prevent,
     * duplicate rules in the output.
     * @param  rule 	input rule to check against the blacklist
     * @return 			rule is blacklisted: true/false
     */
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


    /**
     * Add a rule to the blacklist. 
     * @param rule 	input rule to add to the blacklist
     *
     */
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
