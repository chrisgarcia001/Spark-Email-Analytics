#--------------------------------------------------------------------------------------
# Author: cgarcia
# About: This script is for finding frequent patterns of communication in terms of 
#        email addresses/recipients. The recipient categories (FROM, TO, CC, and BCC) 
#        under consideration may be specified in the parameter file. This script enables 
#        frequent itemsets of communicating email addresses to be identified, and 
#        also allows them to be filtered in any combination of the following:
#        
#        1) Itemsets meeting a minimum specified support level within the dataset
#        2) Itemsets containing only specified subsets of items.
#        
#        See the two corresponding demo parameter files for examples on how to use this.
#--------------------------------------------------------------------------------------

from pyspark import SparkConf, SparkContext
from pyspark.mllib.fpm import FPGrowth
import email_parsing as ep
import sys
import csv_util as csv
import cmd_util as cmd

# Set to this filename to be command line tolerant
THIS_FILENAME = 'frequent_email_comm_pattern_finder.py'


# Print usage instructions.
def print_usage():
	msg = "Usage: > frequent_email_comm_pattern_finder.py -params <parameter file> \n"
	msg += "        ** SEE SAMPLE PARAMETER FILES FOR EXAMPLES \n"
	print(msg)

# For a given itemset and list of target names, determine of the specified number of matches occur.
def itemset_match(itemset, target_names, min_count, max_count):
	count = 0
	for i in itemset:
		for j in target_names:
			if j in i:
				count += 1
	return min_count <= count and max_count >= count
	
# Initialize key variables.
params = None
email_folder = None
target_names = []
min_matches, max_matches = 0, 1000000000000
output_file = None
address_classes = ['to', 'from', 'cc', 'bcc']
min_support = 0.1
num_partitions = 1
min_size, max_size = 0, 1000000000000

# Extract the params and set key variables:
try:
	eval_f = lambda x: csv.standard_eval_input(x, sep=':')  
	params = cmd.read_params(sys.argv, mainfile_suffix=THIS_FILENAME, input_evaluator_f=eval_f)
	email_folder = params['email_folder']
	output_file = params['output_file']
	if params.has_key('target_name_file'):
		target_name_file = open(params['target_name_file'], 'r')	
		target_names = filter(lambda x: not(x in [None, '', ' ', "\t", "\n"]), map(lambda y: y.strip(), target_name_file.readlines()))
		target_name_file.close()
	if params.has_key('target_match_range'):
		min_matches, max_matches = params['target_match_range']
	if params.has_key('itemset_size_range'):
		min_size, max_size = params['itemset_size_range']
	if params.has_key('min_support'):
		min_support = params['min_support']
	if params.has_key('num_partitions'):
		num_partitions = params['num_partitions']
	if params.has_key('address_classes'):
		address_classes = params['address_classes']
		if type(address_classes) != type([]):
			address_classes = [address_classes]
except:
	print_usage()
	exit()

# Main section - mine frequent itemsets, apply filtering, and store results.
conf = SparkConf().setMaster('local').setAppName('Frequent Email Communication Pattern Finder')
sc = SparkContext(conf = conf)
data = sc.wholeTextFiles(params['email_folder'])
transactions = data.map(lambda (file, text): ep.Email(text).select_addresses(address_classes))
model = FPGrowth.train(transactions, minSupport=min_support, numPartitions=num_partitions)
f = lambda x: len(x) >= min_size and len(x) <= max_size and itemset_match(x, target_names, min_matches, max_matches)
itemsets = model.freqItemsets().map(lambda x: x.items).filter(f)
itemsets.saveAsTextFile(params['output_file'])
exit()
	
