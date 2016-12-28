#--------------------------------------------------------------------------------------
# Author: cgarcia
# About: This script is for cardinality-based searching of email addresses/recipients.
#        The recipient categories to include in the search (FROM, TO, CC, and BCC) may 
#        be specified in the parameter file. This script enables two types of cardinality
#        searching which may be combined:
#        
#        1) Finding emails with specified min/max number of total recipients
#        2) Finding emails with specified min/max number of recipients from a given 
#           list.
#        
#        See the two corresponding demo parameter files for examples on how to use this.
#--------------------------------------------------------------------------------------

from pyspark import SparkConf, SparkContext
import email_parsing as ep
import sys
import csv_util as csv

# Print usage instructions.
def print_usage():
	msg = "Usage: > cardinality_email_address_search <parameter file> \n"
	msg += "        ** SEE SAMPLE PARAMETER FILES FOR EXAMPLES \n"
	print(msg)

# Build the search criteria function based on the input args. The resulting function takes in
# raw email text (in standard .eml format) for a single email and returns True or False. 
# The generated criteria function has the following form:  
#   f: <raw email text> -> True | False	
def build_criteria_f(params):
	min_card, max_card = 0, 1000000000000
	if params.has_key('cardinality_range'):
		min_card, max_card = params['cardinality_range']
	ac = ('to', 'from', 'cc', 'bcc')
	if params.has_key('address_classes'):
		ac = params['address_classes']
		if type(ac) != type([]):
			ac = [ac]
	tm, min_match, max_match = '@@@NONEMAILMATCH@@@', 0, 1000000000000
	if params.has_key('target_list'):
		filename =  params['target_list']
		min_match, max_match = params['match_range']
		target_file = open(filename, 'r')
		tm = target_file.readlines()
		target_file.close()
		target_file.close()
	def criteria_f(email_text):
		eml = ep.Email(email_text)
		card = eml.count_addresses(ac)
		matches = eml.count_address_matches(tm, ac)
		return card >= min_card and card <= max_card and matches >= min_match and matches <= max_match
	return criteria_f

# Extract the params and build the criteria function:
params = None
criteria_f = None	
try:
	eval_f = lambda x: csv.standard_eval_input(x, sep=';')
	params = csv.read_params(sys.argv[1], input_evaluator_f=eval_f)
	criteria_f = build_criteria_f(params)
except:
	print_usage()
	exit()

# Perform the search and write the output file.
conf = SparkConf().setMaster('local').setAppName('Cardinality Email Search')
sc = SparkContext(conf = conf)
matches = sc.wholeTextFiles(params['email_folder']).filter(lambda (filename, text): criteria_f(text)).map(lambda (filename, text): filename)
matches.saveAsTextFile(params['output_file'])
exit()
