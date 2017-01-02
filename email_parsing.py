#--------------------------------------------------------------------------------------
# Author: cgarcia
# About: This provides some functions and classes for working with email data.
#--------------------------------------------------------------------------------------

import email

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
		
	
		
		
	
		