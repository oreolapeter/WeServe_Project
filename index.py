
import configparser

import pandas as pd

config = configparser.ConfigParser()

config.read('.env')

WeServe_aws_access = config['AWS']['WeServe_aws_access']

WeServe_aws_secret = config['AWS']['WeServe_aws_secret']


# read in raw dataset to pandas and renaming the columns

call_details_dataset = pd.read_csv(r'call_details.csv',header=0,names=['CallID','callDurationsinSec','AgentGradeLeve','CallType','CallEndedbyAgent'])

call_details_dataset

print('good to go')


#create a copy of the raw dataset

call_details = call_details_dataset.copy()


# DATA CLEANING


# change erroneous entry to correct entry

call_details = call_details.replace('ageentsGradeLevel','')

call_details

call_details=call_details.fillna('1')


# ensuring the uniformity of data by cleaning the irregular entries

call_details['CallType']= call_details['CallType'].replace('in-bound','In-bound')

call_details['CallType'] = call_details['CallType'].replace('In-bound','Inbound')

call_details

# remove whitespaces from all the object types entry

for column in call_details.columns:
    if call_details[column].dtype == 'object':
        call_details[column] = call_details[column].str.strip()

print('error free')

#save the dataset to csv

call_details.to_csv('Cleaned_call_details',index=False)

#read in the data set and rename the column

log_column = ['call_log_id', 'call_id','agent_id','complaint_topic','assigned_to','status','resolution_duration/hr']

call_log_dataset = pd.read_csv(r'call_log.csv',header=0, names=log_column)

call_log_dataset

# create a copy of call log dataset

call_log = call_log_dataset.copy()

#understanding call log dataset

call_log.info()

# fill the empty records in assigned to column

call_log['assigned_to'] = call_log['assigned_to'].fillna(0)

def convert_floattoint(x):
    if type(x)==float:
        x=int(x)
    return x
    
def convert_otherstostr(x):
    if type(x) != str:
        x = str(x)
    return x

# convert assigned to column from flaot data type to int

call_log['assigned_to']=call_log['assigned_to'].apply(convert_floattoint)

# regularise the assigned column by filling appropiately

if (call_log['assigned_to'] == 0).any():
    call_log['assigned_to'] = call_log['agent_id']
else:
    call_log['assigned_to'] = call_log['assigned_to']

# fill the empty records in resolution duration column

call_log['resolution_duration/hr'] = call_log['resolution_duration/hr'].fillna('not yet closed').apply(convert_otherstostr)

# covert the records in status column to lower case for uniformity
def convert_to_lower(x):
    x = x.lower()
    return(x)

call_log['status'] = call_log['status'].apply(convert_to_lower)

# save the cleaned call log dataset

call_log.to_csv('cleaned_call_log',index=False)