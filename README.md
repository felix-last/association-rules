# association-rules
University project utilizing Hadoop MapReduce to derive association rules, a process also known as market basket analysis.

## Run
	$ hadoop_exec run -j bin/AssociationRules.jar -i data/sample.txt --args 200

"--args \<support threshold\>" allows for customization

Support threshold 	: What is the minimum frequency a frequent itemset must have?

## Algorithm
In order to identify association rules, items which are frequently bought together have to be found. Due to its distribution ability, the SON algorithm is used.

1. Find Item Set Candidates

The first step is to find frequent item sets. For this purpose, the a-priori algortihm or one of its extensions is run on each node of the distributed system, each of which holds a part of the entire set of baskets. The support threshold is scaled down by multiplying it with the share of the entire data set held by the node. Every item set that is considered frequent on one of the nodes is considered a candidate set.

2. Evaluate Frequency of Candidates

For each of the candidates found, the frequency in the entire data set is counted in a second pass.

## Architecture
As there are two steps (that is, two passes over the entire data set), there will need to be two sequential MapReduce jobs.

### Class FindCandidates.Mapper
The mapper implements the a-priori algorithm and returns items found to be frequent.

### Class FindCandidates.Reducer
The reducer creates a list of distinct item sets which have been returned by the nodes.

### Class EvaluateCandidates.Mapper
This mapper counts the occurences for each item set contained in the collection returned by the previous reducer.

### Class EvaluateCandidates.Reducer
For each item set, the counts of each node are summed together. This will lead to the final list of frequent item sets, i.e. the basis for deriving association rules.

### Class AssociationRules.AssociationRules (main)
This is the project's main class. It is responsible for calling the other classes and to output the result.

### Class AssociationRules.Util
This class is used for data reading and formatting, as well as other general tasks.

