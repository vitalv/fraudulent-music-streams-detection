#! /usr/bin/python

import os
import subprocess
import pandas as pd
import ast



#Copy all files from bucket

bucket_id = 'ru-0c488479-2436-40a1-9e13-c9831c8a938d'
bucket_id_url = 'gs://ru-0c488479-2436-40a1-9e13-c9831c8a938d'

#cp_cmd = "gsutil cp -r  %s %s"%(bucket_id, os.path.realpath(__file__) )
cp_cmd = "gsutil cp -r  %s %s"%(bucket_id_url, "." )

subprocess.Popen(cp_cmd.split())


#And unzip
#note that I have to add the extension .gz to the file names so I can then gunzip them.

fileNames = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(bucket_id) for filename in filenames]

for fn in fileNames:
	#note that both tracks and users file names are '09'. Change name, move to current folder and add .gz extension:
	if 'tracks'  in fn : subprocess.Popen(['mv', fn, 'tracks.gz'])
	if 'users'   in fn : subprocess.Popen(['mv', fn, 'users.gz'])
	if 'streams' in fn : subprocess.Popen(['mv', fn, 'streams.gz'])

#delete original folder structure:
subprocess.Popen(['rm', '-r', '-f' , bucket_id])

#unzip all:
for f in os.listdir('.'):
	if f.endswith('.gz'):
		subprocess.Popen(['gunzip', f])		







def read_data(filename):
	'''
	reads the contents of streams, tracks and users into a list of dictionaries
	and returns a pandas dataframe
	'''

	lod = [] #list of diction aries

	with open(filename) as f:
		for line in f:
			lod.append(ast.literal_eval(line))

	return pd.DataFrame(lod)

tracks = read_data('tracks')
users = read_data('users')
streams = read_data('streams')


#In streams, group by track_id (and user_id in second level!)
#see tracks repeated more than a n times within a Delta timestamp (by same user)