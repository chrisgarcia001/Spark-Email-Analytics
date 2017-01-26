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
# **NOTE: This file is a single-file form of frequent_email_comm_pattern_finder.py,
#         with all supporting .py file contents copied/pasted in here.
#--------------------------------------------------------------------------------------

from pyspark import SparkConf, SparkContext
from pyspark.mllib.fpm import FPGrowth
import sys
import email

# Set to this filename to be command line tolerant
THIS_FILENAME = 'frequent_email_comm_pattern_finder.py'

# -------------------------------------- CSV UTIL -----------------------------------------------------------

# Convert an excel-style column to an int (e.g. "C" = 3, "AB" = 28)
def to_num(alpha_str):
	try:
		return int(alpha_str) - 1
	except:
		pass
	alpha_str = alpha_str.lower()
	pow = 0
	total = 0
	alpha = 'abcdefghijklmnopqrstuvwxyz'
	inds = range(len(alpha_str))
	inds.reverse()
	for i in inds:
		total += (alpha.index(alpha_str[i]) + 1) * (26 ** pow)
		pow += 1
	return total - 1 # Adjust to 0-based index

# For a list of column numbers (in 1-index numeric or alphabetical format), get
# a corresponding set of a python array indices.
def col_indices(*columns):
	inds = []
	for c in columns:
		cols = filter(lambda x: len(x) > 0, filter(lambda y: y != ' ', str(c)).split('-'))
		if len(cols) == 1:
			inds.append(to_num(cols[0]))
		elif len(cols) > 1:
			inds += range(to_num(cols[0]), to_num(cols[1]) + 1)
	return inds
	
# Write a text file	
def write_file(text, filename):
	f = open(filename, "w")
	f.write(text)
	f.close()
	
# Reads a CSV as a list of rows	
def read_csv(filename, include_headers = True, sep = ',', cleanf = lambda x: x):
	fl = open(filename)
	txt = cleanf(fl.read())
	lines = fl.readlines()
	lines = []
	start_pos = 0 if include_headers else 1
	lines = map(lambda y: y.strip(), txt.split("\n"))[start_pos:]
	return map(lambda x: x.split(sep), filter(lambda y: not(y in ['', ' ', "\t", None]), lines))

# Evaluate and parse an input argument value.
def standard_eval_input(input, sep = ':'):
	is_struct = sep in input
	strings = map(lambda x: x.strip(), input.split(sep))
	vals = []
	for s in strings:
		try:
			iv = int(s)
			ivf = float(s)
			if iv != ivf:
				iv = ivf
			vals.append(iv)
		except:
			if s.lower() == 'true':
				vals.append(True)
			elif s.lower() == 'false':
				vals.append(False)
			else:
				vals.append(s)
	if not(is_struct):
		return vals[0]
	return vals	
	
# Read a CSV file as a param set (dict).
def cv_read_params(filename, sep=',', input_evaluator_f = standard_eval_input):
	if input_evaluator_f == None:
		input_evaluator_f = lambda x: x
	lines = read_csv(filename, sep=sep)
	h = {}
	for line in lines:
		if len(line) > 1 and not(line[0].startswith('#')):
			h[line[0]] = input_evaluator_f(line[1])
	return h
	
# Writes a matrix (2D list) to a CSV file.
def write_csv(matrix, filename, sep = ',', replace_sep = ' '):
	clean_text = lambda s: s.replace(sep, replace_sep)
	text = reduce(lambda x,y: x + "\n" + y, map(lambda row: sep.join(map(clean_text, row)), matrix))
	write_file(text, filename)
	
# Constructs a function which will extract values in a single array
# into multiple arrays. Constant columns will appear in every output array,
# paired with a separate column set in each output. Used to transform lines.
# Example: data = 'abcdefghijklmnopqrstuvwxyz'
#                 f = to_multilinef(['D-G', 'K'], ['P-S', 'U-V'], ['m-o'])
#                 f(data)
#  -->    [['p', 'q', 'r', 's', 'u', 'v', 'd', 'e', 'f', 'g', 'k'], 
#          ['m', 'n', 'o', 'd', 'e', 'f', 'g', 'k']]
def to_multilinef(constant_cols, *col_sets):
	fixed_cols = col_indices(*constant_cols)
	fixed_lines = lambda arr: [arr[i] for i in fixed_cols]
	var_col_sets = map(lambda cs: col_indices(*cs), col_sets)
	all_var_lines = lambda arr: map(lambda vs: [arr[i] for i in vs], var_col_sets)
	var_lines = lambda arr: filter(lambda line: len(map(lambda y: not(y in ['', ' ']), line)) > 0, all_var_lines(arr))
	return lambda arr: map(lambda x: x + fixed_lines(arr), var_lines(arr)) 
	
# -------------------------------------- CMD UTIL -----------------------------------------------------------

def read_cmd_params(cmd_args, mainfile_suffix=None, key_prefix='-', input_evaluator_f=standard_eval_input):
	ref = 0
	if mainfile_suffix != None:
		while ref < len(cmd_args) and not(cmd_args[ref].endswith(mainfile_suffix)):
			ref += 1
	inds = range(ref, len(cmd_args))
	key_inds = filter(lambda i: cmd_args[i].startswith(key_prefix), inds)
	keys = map(lambda x: x[len(key_prefix):], map(lambda y: cmd_args[y], key_inds))
	vals = map(lambda x: input_evaluator_f(cmd_args[x + 1]), key_inds)
	return dict(zip(keys, vals))	
	
# Performs smart param reading by detecting whether the info in cmd_args points to a CSV parameter file 
# or whether it contains the params itself. It does so by looking to see if cmd_args contains a key_inds
# named whatever is contained in csv_param_designator (e.g., defaults to see if there is a key called 'params').
# The rest of the arguments are as shown above.	
def read_params(cmd_args, mainfile_suffix=None, key_prefix='-', input_evaluator_f=standard_eval_input, csv_param_designator='params'):
	params = read_cmd_params(cmd_args, mainfile_suffix, key_prefix, input_evaluator_f)
	if params.has_key(csv_param_designator):
		return cv_read_params(params['params'], input_evaluator_f=input_evaluator_f)
	return params


# -------------------------------------- EMAIL PARSING ------------------------------------------------------

# Remove repeated spaces.
def normalize_spaces(text):
	while '  ' in text:
		text = text.replace('  ', ' ')
	return text

# Provide a simple normalization for email address strings.
def normalize_email_address_text(text):
	if text == None:
		return ''
	return normalize_spaces(text.lower())

# Split a string of email addresses into the individual addresses, properly normalized.	
def normalized_email_addresses(text):
	return map(lambda x: x.strip(), normalize_email_address_text(text).split(','))

# Parse an individual email address. Returns a tuple in form (name, email_address)
def parse_email_address(address_text):
	address_text = address_text.replace('"', '').replace("'", '').replace("\n", ' ').replace("\t", ' ').replace('>', '').lower()
	components = map(lambda x: x.strip(), filter(lambda y: not(y in ['', None]), address_text.split('<')))
	cps = []
	for c in components:
		c = normalize_spaces(c)
		if not('@' in c):
			c.replace('.', '')
		cps.append(c)
	if len(cps) < 2:
		cps = [None] + cps
	return tuple(cps)

# Parse a combined set of email addresses, separated by commas or semicolons.	
def parse_email_addresses(address_text):
	if address_text == None:
		return []
	return map(parse_email_address, address_text.replace(';', ',').split(','))

# A class for working with email.	
class Email:
	def __init__(self, raw_eml_text):
		self.email_addresses = {}
		self.email_addresses['to'], self.email_addresses['from'], self.email_addresses['cc'], self.email_addresses['bcc'] = [],[],[],[]
		self.body = ''
		try:
			self.raw_text = raw_eml_text
			msg = email.message_from_string(raw_eml_text)
			self.parsed_message = msg
			ads = map(normalized_email_addresses, [msg['to'], msg['from'], msg['cc'], msg['bcc']])
			self.email_addresses['to'], self.email_addresses['from'], self.email_addresses['cc'], self.email_addresses['bcc'] = ads
			if msg.is_multipart():
				self.body = msg.get_payload()[0]
			else:
				self.body = msg.get_payload()
		except:
			pass

	# Print the contents of this email - mainly for debugging.
	def display(self):
		print('TO: ' + str(self.email_addresses['to']))
		print('FROM: ' + str(self.email_addresses['from']))
		print('CC: ' + str(self.email_addresses['cc']))
		print('BCC: ' + str(self.email_addresses['bcc']))
		print('Body: \n------\n' + str(self.body) + '--------\n')
	
	# Select the unique email addresses from the specified classes.
	def select_addresses(self, classes = ['to', 'from', 'cc', 'bcc']):
		em = []
		for c in classes:
			em += self.email_addresses[c.lower()]
		return filter(lambda x: not(x in [None, '', ' ', "\t", "\n"]), list(set(em)))
	
	# Count the email addresses in the specified classes.
	def count_addresses(self, classes = ['to', 'from', 'cc', 'bcc']):
		return len(self.select_addresses(classes))
	
	# For a given address or list of addresses, get the number found in this email within the specified classes.
	def count_address_matches(self, addresses, classes = ['to', 'from', 'cc', 'bcc']):
		if type(addresses) == type([]):
			addresses = ','.join(addresses)
		addresses = list(set(normalized_email_addresses(addresses)))
		internal_addresses = ','.join(self.select_addresses(classes))
		matches = filter(lambda x: x in internal_addresses, addresses)
		return len(matches)
		


# -------------------------------------- MAIN ---------------------------------------------------------------


# Print usage instructions.
def print_usage():
	msg = "Usage: > cb_frequent_email_comm_pattern_finder.py -params <parameter file> \n"
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
	eval_f = lambda x: standard_eval_input(x, sep='::')  
	params = read_params(sys.argv, mainfile_suffix=THIS_FILENAME, input_evaluator_f=eval_f)
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
transactions = data.map(lambda (file, text): Email(text).select_addresses(address_classes))
model = FPGrowth.train(transactions, minSupport=min_support, numPartitions=num_partitions)
f = lambda x: len(x) >= min_size and len(x) <= max_size and itemset_match(x, target_names, min_matches, max_matches)
itemsets = model.freqItemsets().map(lambda x: x.items).filter(f)
itemsets.saveAsTextFile(params['output_file'])
exit()
	