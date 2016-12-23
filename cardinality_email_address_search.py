#--------------------------------------------------------------------------------------
# Author: cgarcia
# About: This script is for cardinality-based searching of email addresses/recipients.
#        The recipient categories to include in the search (FROM, TO, CC, and BCC) may 
#        be specified in the command line. This script enables two types of cardinalty
#        searching which may be combined:
#        1) Finding emails with specified min/max number of total recipients
#        2) Finding emails with specified min/max number of recipients from a given 
#           list.
#--------------------------------------------------------------------------------------

from pyspark import SparkConf, SparkContext
import email_parsing as ep
import sys

# Print usage instructions.
def print_usage():
	msg = "Usage: > cardinality_email_address_search <directory of emails> <output file> [-cardinality_range <min>:<max>]\n"
	msg += "        [-target_list <list of names/email addresses>] [-match_range <min>:<max>]\n"
	msg += "        [-address_classes FROM:TO:CC:BCC **Default=All]"
	print(msg)

# Evaluate and parse an input argument value.
def eval_input(struct, sep = ':'):
	is_struct = sep in struct
	strings = struct.split(':')
	vals = []
	for s in strings:
		try:
			iv = int(s)
			vals.append(iv)
		except:
			vals.append(s)
	if not(is_struct):
		return vals[0]
	return vals	

# Build input args into a parameter set.
def parse_args(args):
	params = {}
	params['email_folder'] = args[1]
	params['output_file'] = args[2]
	i = 3
	while i < len(args):
		key = args[i][1:]
		print(key)
		val = eval_input(args[i + 1])
		params[key] = val
		i += 2
	return params

# Build the search criteria function based on the input args. The resulting function takes in
# raw email text (in standard .eml format) for a single email and returns True or False. 
# The generated criteria function has the following form:  
#   f: <raw email text> -> True | False	
def build_criteria_f(params):
	emf = params['email_folder']
	output_file = params['output_file']
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
	params = parse_args(sys.argv)
	criteria_f = build_criteria_f(params)
except:
	print_usage()
	exit()
	
# Perform the search and write the output file.
conf = SparkConf().setMaster('local').setAppName('Cardinality Email Search')
sc = SparkContext(conf = conf)
matches = sc.wholeTextFiles(params['email_folder']).filter(lambda (filename, text): criteria_f(text)).map(lambda (filename, text): filename)
matches.saveAsTextFile(params['output_file'])
#for path in matches.collect():
#	print(path)
exit()
