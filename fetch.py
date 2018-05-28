#! /usr/bin/python

import os
import subprocess


bucket_id = 'ru-0c488479-2436-40a1-9e13-c9831c8a938d'
bucket_id_url = 'gs://ru-0c488479-2436-40a1-9e13-c9831c8a938d'
current_folder = os.path.dirname(os.path.abspath(__file__))


def fetch_data():

	cp_cmd = "gsutil cp -r  %s %s"%(bucket_id_url, current_folder)
	subprocess.Popen(cp_cmd.split()).communicate()


def move_rename():

	bucket_path = os.path.join(current_folder, bucket_id)

	#get file names with the complete path
	fileNames = [os.path.join(dirpath, filename) for dirpath, dirnames, filenames in os.walk(bucket_path) for filename in filenames]

	#move/rename to current directory
	print fileNames
	print "Moving tracks, users and streams files to current directory"
	for fn in fileNames:
		#note that both tracks and users file names are '09'. Change name, move to current folder and add .gz extension:
		if 'tracks'  in fn : subprocess.Popen(['mv', fn, 'tracks.gz'])
		if 'users'   in fn : subprocess.Popen(['mv', fn, 'users.gz'])
		if 'streams' in fn : subprocess.Popen(['mv', fn, 'streams.gz'])

	#and delete original folder
	subprocess.Popen(['rm', '-r', '-f' , bucket_path]).communicate()


def gunzip():
	
	print "Unzipping tracks.gz, users.gz and streams.gz"
	for f in os.listdir(current_folder):
		if f.endswith('.gz'):
			subprocess.Popen(['gunzip', f]).communicate()
			subprocess.Popen(['rm', f]).communicate()


def main():

	fetch_data()
	move_rename()
	#delete original folder structure:
	
	gunzip()


if __name__ == '__main__':
	main()
