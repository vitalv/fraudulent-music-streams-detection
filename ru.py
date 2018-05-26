#! /usr/bin/python

import os
import subprocess
import pandas as pd


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





lod = [] #list of dictionaries

with open('tracks') as f:
	for line in f:
		lod.append(line)

pd.