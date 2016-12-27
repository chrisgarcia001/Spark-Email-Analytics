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


# Print usage instructions.
def print_usage():
	msg = "Usage: > frequent_email_comm_pattern_finder.py <parameter file> \n"
	msg += "        ** SEE SAMPLE PARAMETER FILES FOR EXAMPLES \n"
	print(msg)
	
# Initialize key variables.
params = None
email_folder = None
target_patterns = None
output_file = None
min_support = 0.1
num_partitions = -1
min_size, max_size = 0, 1000000000000

# Extract the params and set key variables:
try:
	eval_f = lambda x: csv.standard_eval_input(x, sep=';')
	params = csv.read_params(sys.argv[1], input_evaluator_f=eval_f)
	email_folder = params['email_folder']
	output_file = params['output_file']
	if params.has_key('target_pattern_file'):
		target_pattern_file = open(filename, 'r')	
		tp = map(lambda x: x.replace(',', ';').split(';'), target_pattern_file.readlines())
		target_pattern_file.close()
		tp = map(lambda x: filter(lambda y: not(y in ['', ' ', "\t", None]), map(lambda z: z.strip(), x)), tp)
		target_patterns = tp
	if params.has_key('min_support'):
		min_support = params['min_support']
	if params.has_key('num_partitions'):
		num_partitions = params['num_partitions']
	if params.has_key('pattern_size_range'):
		min_size, max_size = params['pattern_size_range']
except:
	print_usage()
	exit()

# TODO - Implement the main part below!
conf = SparkConf().setMaster('local').setAppName('Cardinality Email Search')
sc = SparkContext(conf = conf)
matches = sc.wholeTextFiles(params['email_folder']).filter(lambda (filename, text): criteria_f(text)).map(lambda (filename, text): filename)
matches.saveAsTextFile(params['output_file'])
exit()
	
