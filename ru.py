#! /usr/bin/python

import pandas as pd
import ast



def read_data(filename):
	'''
	reads the contents of streams, tracks and users into a list of dictionaries
	and returns a pandas dataframe
	'''

	lod = [] #list of dictionaries

	with open(filename) as f:
		for line in f:
			lod.append(ast.literal_eval(line))

	return pd.DataFrame(lod)

tracks = read_data('tracks')
users = read_data('users')
streams = read_data('streams')



#The objective is to build a classifier to identify accounts likely belonging to bots
#To create the labels one could look for 'red flags'/giveaways. And combine those creating a scoring method
#to measure the likelihood of an account being a bot 



#check distribution of streams / user

stream_counts = streams.groupby(['user_id', 'track_id']).size().reset_index(name='counts').sort_values(by='counts', ascending=False)



#If I look at the users in top of stream_counts 
users[users.user_id == '8ec3584b7ef681f6b817c6b2a5b54153011d7efa']
#I see most of them are from country 59



#stream_counts.head(1)

#Track 25b1e75619a00b47119dbdd88bf4a37b854eedba is present 2339 times in streams
#out of those 2339 times, user bbc91687167393872dcb557f6413e4acfed3ba77 has streamed it 2333 times.

#In total that track is present 2339 times in streams
len(streams[streams.track_id == '25b1e75619a00b47119dbdd88bf4a37b854eedba'])

#2333 of those times were streamed by just one user. In total only 3 users have streamed it
set(streams[streams.track_id == '25b1e75619a00b47119dbdd88bf4a37b854eedba'].user_id) 
#All 3 of them are from country 59






#Potential Red Flags

#In streams, group by track_id (and user_id in second level!)
#see tracks repeated more than a n times within a Delta timestamp (by same user)

#Distribution of the number of streams per user. Has to be normal

#For a user see if the tracks they stream are equally distributed. 
#E.g. a user has 1000 streams and it's exactly 4 songs 250 times each. -> giveaway


#For a user see if the tracks they stream have the exact same time stamp