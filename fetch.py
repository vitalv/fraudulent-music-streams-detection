#! /usr/bin/python

import os
import subprocess


bucket_id = 'ru-0c488479-2436-40a1-9e13-c9831c8a938d'
bucket_id_url = 'gs://ru-0c488479-2436-40a1-9e13-c9831c8a938d'
current_folder = os.path.dirname(os.path.abspath(__file__))


def fetch():

	cp_cmd = "gsutil cp -r  %s %s"%(bucket_id_url, current_folder)
	subprocess.Popen(cp_cmd.split()).communicate()


def move_rename():

	#get file names with the complete path
	fileNames = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(bucket_id) for filename in filenames]

	#and move/rename to current directory
	print "Moving tracks, users and streams files to current directory"
	for fn in fileNames:
		#note that both tracks and users file names are '09'. Change name, move to current folder and add .gz extension:
		if 'tracks'  in fn : subprocess.Popen(['mv', fn, 'tracks.gz'])
		if 'users'   in fn : subprocess.Popen(['mv', fn, 'users.gz'])
		if 'streams' in fn : subprocess.Popen(['mv', fn, 'streams.gz'])


def gunzip():
	
	print "Unzipping tracks.gz, users.gz and streams.gz"
	for f in os.listdir(current_folder):
		if f.endswith('.gz'):
			subprocess.Popen(['gunzip', f]).communicate()



def main():

	fetch()
	move_rename()
	#delete original folder structure:
	subprocess.Popen(['rm', '-r', '-f' , bucket_id]).communicate()
	gunzip()


if __name__ == '__main__':
	main()
