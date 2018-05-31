#! /usr/bin/python

import numpy as np
import pandas as pd
import ast
import collections
import seaborn as sb
import matplotlib.pyplot as plt


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
#But to do that we need first labels to define categories: human user / bot
#To create the labels one could look for potential 'red flags'/giveaways. And combine those creating a scoring method
#to measure the likelihood of an account being a bot 
#Those red flags are parameters that can be assigned to users (Dataframe: Users x RedFlagParameters)
#Using the scoring method select a subset of users that are likely bots and a subset of users likely humans. Combine it into a train dataset
#Build some classifiers SVD, DecisionTrees, ANN, ... and apply on the test dataset (users)



#PRF 1: (Potential Red Flag) Parameter NFSC Normalized Frequency of Streams per Country:
'''
This was motivated by the fact that the users in top of stream_counts 
Eg. users[users.user_id == '8ec3584b7ef681f6b817c6b2a5b54153011d7efa']
come mainly from a couple of countries. Interesting
Now sorting the user accounts by number of streams doesn't really prove anything
But maybe some insights can be acquired 
Wait, there might be real human users from one country that are just very heavy users. Yes
But one could normalize the frequency in the top n to the frequency in a background random n streams

'''
streams = pd.merge(streams, users, on='user_id')
streams = pd.merge(streams, tracks, on='track_id')

stream_counts = streams.groupby(['user_id', 'track_id']).size().reset_index(name='counts')
#stream_counts =	stream_counts.sort_values(by='counts', ascending=False)
#Gets number of streams per user.
#streams here is a track played by a user
#stream counts is the number of times a track is played by a user


def nfsc(stream_counts, group_by='country', top_n=100):

	'''
	Normalized Frequency of Stream Counts
	group_by is one of ['country', 'os', 'access']
	Get frequencies of streams per ['country', 'os', 'access'] in the top_n stream counts 
	and normalize it to the background (random n) streams
	'''
	#stream_counts = streams.groupby(['user_id', 'track_id']).size().reset_index(name='counts')
	#stream_counts.sort_values(by='counts', ascending=False)

	#Countries of origin for the top n users in stream_counts:
	top_streams_users = stream_counts.sort_values(by='counts', ascending=False).head(top_n).user_id
	#top_streams_users_countries = [users[users.user_id == u].country.values[0] for u in top_streams_users.values]
	top_streams_users_grouped = [streams[streams.user_id == u][group_by].values[0] for u in top_streams_users]

	plt.figure(figsize=(20,12))
	#ax = sb.distplot(top_streams_users_countries, bins=len(list(set(users.country))), kde=False)
	ax = sb.countplot(top_streams_users_countries)
	plt.xlabel(group_by)
	plt.title('%s for the Top %s streams'%(group_by, top_n))
	plt.savefig('plots/%s_for_top_%s_streams.png'%(group_by, top_n))
	#plt.show()

	#rand_streams_users_grouped = [users[users.user_id == u].country.values[0] for u in streams.sample(top_n).user_id]
	rand_streams_users_grouped = [streams[streams.user_id == u][group_by].values[0] for u in streams.sample(top_n).user_id]

	plt.figure(figsize=(20,12))
	#sb.distplot(rand1000_streams_users_countries, bins=users.country.max(), kde=False)
	ax = sb.countplot(rand_streams_users_grouped)
	plt.xlabel(group_by)
	plt.title('%s for a random sample of %s streams'%(group_by,top_n))
	#plt.show()
	plt.savefig('plots/%s_for_rand_%s_streams.png'%(group_by,top_n))


	#Normalize frequency in top1000 to freq in rand1000
	freq_top = collections.Counter(top_streams_users_grouped)
	freq_rand = collections.Counter(rand_streams_users_grouped)

	nfsc = {}
	#if nfsc > 1 :  the group(e.g. country) is 'over-represented' in the top1000 streams compared to the background rand1000
	#if nfsc == 1:  equal frequency in the top1000 and rand1000
	#if nfsc < 1 :  the frequency of the country in the top1000 streams is more or less random

	for i in list(set(streams[group_by])):
		f_top   = freq_top[i]  / float(top_n)
		f_rand  = freq_rand[i] / float(top_n)
		try:
			nfsc[i] = f_top / f_rand
		except ZeroDivisionError:
			nfsc[i] = 1.0

	return nfsc


nfsc100_country  = nfsc(stream_counts, group_by='country', top_n = 100)
nfsc1000_country = nfsc(stream_counts, group_by='country', top_n = 1000)








#Build a new users df with columns:
#nsfc100_country, nfsc1000_country, nfsc100_os, nfsc1000_os, nfsc1000_os, nfsc100_access, nfsc1000_access, label













#----------------------------------------------------

#CHECK distribution of stream lengths
#see if there is a peak at 30 sec
#sb.distplot(streams.length.values)




















#PRF 2: (Potential Red Flag) Track-User Enrichment Analysis
'''

Based on the following observation. 

Sort the stream counts (number of times a track is played by one user) : stream_counts.head(1)

Track '25b1e75619a00b47119dbdd88bf4a37b854eedba' is present 2339 times in streams: len(streams[streams.track_id == '25b1e75619a00b47119dbdd88bf4a37b854eedba'])
out of those 2339 times, user bbc91687167393872dcb557f6413e4acfed3ba77 has streamed it 2333 times.

2333 of those times were streamed by just one user. In total only 3 users have streamed it
set(streams[streams.track_id == '25b1e75619a00b47119dbdd88bf4a37b854eedba'].user_id) 
All 3 of them are from country 59


This type of analysis links users with artists (or tracks/albums). 
The goal is to see whether an artist is over-represented in the streams
by one particular user (or group of users) compared to the background (times the artist is played by all other users)

for every user (or group of users) find:

				#streams artist1 by group		#streams total by group		#streams artist1 total		#streams in total
artist1					x 								k 							m                       N
artist2


N: Total number of streams
k: Number of streams by one particular user
m: Number of streams of one particular artist(or album)
x: Number of streams of the one particular artist (or album) by the one particular user


The hypergeometric test (Fisher's exact test) provides the probability that 
in 'k' random observations from 'N' there will be 'x' observations matching the test (streams by artist)
This probability is a p-value, it can be intepreted this way:
Having a very low p-value means the the group's streams are 'enriched' in that particular artist, 
or in other words, the artist is 'over-represented' in that group considering the background (total streams by all users) 


IMPORTANT NOTE!:
What about users with very 'niche' taste: Only them (and nobody else) play ONLY very few artists (AND lots of times)
Right. These user accounts will get low p-values (which are ideally intended to represent bots). 
On the positive side:
- This behaviour shouldn't be too common among real human users (Arguable)
- This could be alleviated by introducing thresholds:
	In the minimum stream count
	In the minimum number of countries of origin
- Repeating the enrichment for different groups of users (based on OS, Country, Access) could give a COMBINATION score that could be more robust

Why would users from a certain country listen more to one artist? -> This has a plausible explanation. E.g. A group of South Korean users will have very low p-values for a K-pop-star
Why would users of a certain OS listen more to one artist? -> This is more strange, but it could happen
Why would users with a certain type of account ('free' or 'premium') would listen more to one artist? Same, it could happen

Combining all these it might be possible to find a pattern. Interesting!

'''


#add 'user_country', 'artist' and 'user_access' to streams for comodity
#otherwise, every time I need to set k based on something that is not in 'streams' E.g. users from a country
#I need to first find the users from the country 
#uic = users[users.country == c].user_id
#and then k would be 
#streams[streams.user_id.isin(uic)] #which takes very long! and it needs be done for every group of users!

streams = pd.merge(streams, users, on='user_id')
streams = pd.merge(streams, tracks, on='track_id')

from scipy import stats
import statsmodels.sandbox.stats.multicomp as mc



def enrich(k, N, tests, tests_desc, xmin=10, top_N=1000):

	'''
	tests_desc: what is tested (artist, track, album). Has to be an existing column in streams (the extended streams w all columns)
	uses scipy.stats hypergeometric test to extract probabilities (p-values) of track/album enrichment for a user/group of users
	k: column by which users are grouped (os, country, access,...)
	tests: what is tested for enrichment: artists, album, track...
	xmin: minimum number of times the 'artist' has to be present in the streams by the user
	top_N: add only top N results (lowest p-values) to output dataframe
	'''

	enrichment = pd.DataFrame(columns=["artist", "x", "k", "m", "N", "p-val" ])
	for i in range(len(tests)):
		x = len(streams[(streams.os == os) & (streams[tests_desc] == tests[i])])
		if x > xmin: 
			m = len(streams[streams[tests_desc] == tests[i]])
			p = stats.hypergeom.sf(x, N, k, m)
			enrichment.loc[i] = [tests[i], x, k, m, N, p]
			print "%s\tx:%s k:%s m:%s N:%s p:%s"%(tests[i], x,k,m,N,p)
	#Multiple hypothesis correction, transform p-values to adjusted p-values:
	if len(tests) > 100:
		reject, adj_pvalues, corrected_a_sidak, corrected_a_bonf =  mc.multipletests(enrichment["p-val"], method='fdr_bh')
		enrichment["adj_pval(BH)"] = adj_pvalues
		enrichment = enrichment.sort_values(by='adj_pval(BH)').head(top_N)

	return enrichment




#k users grouped by OS. See if the k groups of users have some enrichment in some artist

artists = list(set(tracks.album_artist)) #tests for enrich function

#Filter 'm' (total number of times that an artist is streamed) 
#to be at least 100. The results will be more meaningful and the enrichment will go much faster
artists_100 = [a for a in artists if len(streams[streams.album_artist == a]) > 100]


for os in list(set(streams.os)):
	print os
	k=len(streams[streams.os==os])
	N=len(streams)
	enrichment = enrich(k, N, tests=artists_100, tests_desc='album_artist')
	print enrichment
	enrichment.to_csv('enrichment/%s_artist_enrichment.csv'%os)


#for every type of grouping (os, country, access)
#this takes long, parallelize

#Read the resulting dataframes 
	#keep only adjusted p-values < .05
	#add  column: type of group 'os'
	#concatenate (axis=0)


#Repeat the enrichment analysis for country, and access
#Read the resulting dataframes adding column: 'country' and 'access'

for c in list(set(streams.country)):
	print country
	enrichment = enrich(k=len(streams[streams.country==c]), N=len(streams), tests=artists_100, tests_desc='country')
	print enrichment
	enrichment.to_csv('enrichment/%s_artist_enrichment.csv'%c)



#Merge resulting dataframes on artist (add new p-val columns for 'os', 'country', 'access' with suffix)

#Combine the p-values somehow into a new column

#Sort by combination-score. Find a few good p-values (low)

#In the extended 'streams' df find users with 'artist', 'os', 'country', 'access' of those low p-values
#and Label those motherfuckers as potential bots
















#Potential Red Flags

#In streams, group by track_id (and user_id in second level!)
#see tracks repeated more than a n times within a Delta timestamp (by same user)

#Distribution of the number of streams per user. Has to be normal

#For a user see if the tracks they stream are equally distributed. 
#E.g. a user has 1000 streams and it's exactly 4 songs 250 times each. -> giveaway


#For a user see if the tracks they stream have the exact same time stamp