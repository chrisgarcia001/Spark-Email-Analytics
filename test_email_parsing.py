#--------------------------------------------------------------------------------------
# Author: cgarcia
# About: # A few quick tests for the email_parsing module.
#--------------------------------------------------------------------------------------

from email_parsing import *

test_email = 'C:/DataSets/basic_email/msg_2.eml'
test_email = 'test_email_data/69_'
email_file = open(test_email, 'r')
email_string = email_file.read()
email_file.close()
msg = email.message_from_string(email_string)

#print(msg['to'])
#print(parse_email_addresses(msg['to']))

email = Email(email_string)
email.display()

ext_addresses = ['ruth.concannon@enron.com', 'howard.sangwine@enron.com', '   ryan.watt@enron.com']
print(email.count_address_matches(ext_addresses))
print(email.count_address_matches(ext_addresses, ['from']))
print(email.count_address_matches(ext_addresses, ['TO']))
