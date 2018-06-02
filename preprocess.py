#! /usr/bin/python
import os
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

#add 'users', and 'tracks' information to 'streams' to have all the info in one df 
streams = pd.merge(streams, users, on='user_id')
streams = pd.merge(streams, tracks, on='track_id')





#The goal is to build a classifier to identify accounts likely belonging to bots
#But to do that we need first labels to define categories: human user / bot
#To create the labels one could look for potential 'red flags'/giveaways. And combine those creating a scoring method
#to measure the likelihood of an account being a bot 
#Those red flags are parameters that can be assigned to users (Dataframe: Users x RedFlagParameters)
#Using the scoring method select a subset of users that are likely bots and a subset of users likely humans. Combine it into a train dataset
#Build some classifiers SVD, DecisionTrees, ANN, ... and apply on the test dataset (users)



#Find Normalized Frequency of Streams (per 'country', 'os', 'access', 'device'):
'''
This was motivated by the fact that the users in top of stream_counts 
Eg. users[users.user_id == '8ec3584b7ef681f6b817c6b2a5b54153011d7efa']
come mainly from a couple of countries. Interesting
Now sorting the user accounts by number of streams doesn't really prove anything
But maybe some insights can be acquired 
Wait, there might be real human users from one country that are just very heavy users. Yes
But one could normalize the frequency in the top n to the frequency in a background random n streams


'''

stream_counts = streams.groupby(['user_id', 'track_id']).size().reset_index(name='counts')
#stream_counts =	stream_counts.sort_values(by='counts', ascending=False)
#Gets number of streams per user.
#streams here is a track played by a user
#stream counts is the number of times a track is played by a user


def nfsc(streams, group_by, top_n):

	'''
	Normalized Frequency of Stream Counts
	group_by is one of ['country', 'os', 'access']
	Get frequencies of streams per ['country', 'os', 'access'] in the top_n stream counts 
	and normalize it to the background (random n) streams
	'''
	#stream_counts = streams.groupby(['user_id', 'track_id']).size().reset_index(name='counts')
	#stream_counts.sort_values(by='counts', ascending=False)

	#Get the top n users in stream_counts:
	top_streams_users = stream_counts.sort_values(by='counts', ascending=False).head(top_n).user_id
	#And get the country/os/access of those users
	top_streams_users_grouped = [streams[streams.user_id == u][group_by].values[0] for u in top_streams_users]

	plt.figure(figsize=(20,12))
	ax = sb.countplot(top_streams_users_grouped)
	plt.xlabel(group_by)
	plt.title('%s for the Top %s streams'%(group_by, top_n))
	plt.savefig('plots/%s_for_top_%s_streams.png'%(group_by, top_n))
	#plt.show()

	#Get the country/os/access of a random group of users (the same size as top_n)
	rand_streams_users_grouped = [streams[streams.user_id == u][group_by].values[0] for u in streams.sample(top_n).user_id]

	plt.figure(figsize=(20,12))
	ax = sb.countplot(rand_streams_users_grouped)
	plt.xlabel(group_by)
	plt.title('%s for a random sample of %s streams'%(group_by,top_n))
	plt.savefig('plots/%s_for_rand_%s_streams.png'%(group_by,top_n))
	#plt.show()


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



def add_nfsc_to_streams():

	#this takes quite long time. TO DO: Parallelize (see class MyPool below)

	for g in ['country', 'os', 'access', 'device_type', 'gender']:
		nfsc100 = nfsc(streams, group_by=g, top_n = 100)
		nfsc500 = nfsc(streams, group_by=g, top_n = 500)
		streams['nfsc100_%s'%g] = [nfsc100[c] for c in streams[g]]
		streams['nfsc500_%s'%g] = [nfsc500[c] for c in streams[g]]

	#add a combined nfsc_score
	cols = [c for c in streams.columns if 'nfsc' in c]
	streams['nfsc_score'] = streams[cols].mean(axis=1)
	return streams.sort_values(by='nfsc_score', ascending=False)




#with the df resulting from add_nfsc_to_streams:
streams['label'] =  [1]*100 + [0]*(len(streams)-100)

#build a new df (train dataset) based on the considered features: country os access device_type gender
dfX = streams[['country', 'os', 'access', 'device_type', 'gender', 'label']]

#convert all categorical values to numbers and scale them to be between 0 and 1 for instance
for g in ['os', 'access', 'device_type', 'gender']:
	d = dict(zip(set(dfX[g]),range(len(set(dfX[g])))))
	#set a number for each categorical value. (eg: 'iOS': 0, 'Android': 1, etc...)
	dfX[g] = [d[c] for c in dfX[g]] 
	#and scale those numbers to be between 0 and 1
	dfX[g] = [(c - min(dfX[g])) / float(max(dfX[g]) - min(dfX[g])) for c in dfX[g]]







# An extension of the previous nfsc concept is the more robust statistical approach described below:
# 'Over-representation' or 'Enrichment' of one artist/band in the streams by a group of users
# Provides a way to classify artists as potentially responsible for fraud (bots loop-streaming)
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


from scipy import stats
import statsmodels.sandbox.stats.multicomp as mc



def enrich(group_by, g, k, N, tests, tests_desc, xmin=10, top_N=1000):

	'''
	group_by: the column on which the grouping of k is based ('os', 'country', 'access' )
	g: the group in particular ('Android', 'iOS', etc..)
	tests_desc: what is tested (artist, track, album). Has to be an existing column in streams (the extended streams w all columns)
	uses scipy.stats hypergeometric test to extract probabilities (p-values) of track/album enrichment for a user/group of users
	k: column by which users are grouped (os, country, access,...)
	tests: what is tested for enrichment: artists, album, track...
	xmin: minimum number of times the 'artist' has to be present in the streams by the user
	top_N: add only top N results (lowest p-values) to output dataframe
	'''

	enrichment = pd.DataFrame(columns=["g", tests_desc, "x", "k", "m", "N", "p-val"])
	for i in range(len(tests)):
		x = len(streams[(streams[group_by] == g) & (streams[tests_desc] == tests[i])])
		if x > xmin: 
			m = len(streams[streams[tests_desc] == tests[i]])
			p = stats.hypergeom.sf(x, N, k, m)
			enrichment.loc[i] = [g, tests[i], x, k, m, N, p]
			print "%s\t%s\tx:%s k:%s m:%s N:%s p:%s"%(g,tests[i], x,k,m,N,p)
	#Multiple hypothesis correction, transform p-values to adjusted p-values:
	if len(tests) > 100:
		reject, adj_pvalues, corrected_a_sidak, corrected_a_bonf =  mc.multipletests(enrichment["p-val"], method='fdr_bh')
		enrichment["adj_pval(BH)"] = adj_pvalues
		enrichment = enrichment.sort_values(by='adj_pval(BH)').head(top_N)

	return enrichment







artists = list(set(tracks.album_artist)) #tests for enrich function
#Filter 'm' (total number of times that an artist is streamed) 
#to be at least 100. The results will be more meaningful and the enrichment will go much faster
artists_100 = [a for a in artists if len(streams[streams.album_artist == a]) > 100]





def batchEnrich(processes = None):


	'''
	for every type of grouping (os, country, access)
	the enrichment takes quite long time (1hr approx on my laptop (i7 8Gb), parallelize
	this will start a new process for every type of 'os' (5 processes)... 
	and then the same for every 'country', 'access'
	'''

	if not os.path.isdir('enrichment'):
		os.mkdir('enrichment')
  
  	'''
  	# TO DO:
	# parallelize the enrichment function #see MyPool class below

	pool = MyPool(processes) 


	for g in ['os', 'country', 'access']:
		print("  Enrichment analysis for %s"%g)
		for 
		k=len(streams[streams[g]==os])
		N=len(streams)
		pool.applyAsync(enrich, [ms1File, ms1Folder, ms2Folder, beFolder])
		pool.checkPool()
	'''


	N=len(streams)
	tests_desc='album_artist'
	for group_by in ['os', 'access', 'country']: #the column on which the grouping of k is based ('os', 'country', 'access' )
		for g in list(set(streams[group_by])):   #the group in particular ('Android', 'iOS', etc..)
			print "\n\n%s"%g
			k = len(streams[streams[group_by]==g])
			enrichment = enrich(group_by, g, k, N, tests=artists_100, tests_desc=tests_desc)
			enrichment.to_csv('enrichment/%s/%s_%s_enrichment.csv'%(group_by,g,tests_desc))





def combine():

	'''
	concatenates (along rows) all dfs from the enrichment from each group.
	Ex: concatenates iOS, Android, Mac, Linux... to a long Df
	Three resulting long dfs: OS, Access, Country.
	Combines the p-values
	'''

	df_group_by_List = []
	for group_by in ['os', 'access', 'country']:
		dfgList = []
		for f in os.listdir('enrichment/%s/'%group_by):
			dfg = pd.read_csv('enrichment/%s/%s'%(group_by, f), usecols=range(1,9))
			dfg = dfg[dfg['adj_pval(BH)']<0.1][['album_artist', 'g', 'adj_pval(BH)']]
			dfgList.append(dfg)
		dfg = pd.concat(dfgList)
		df_group_by_List.append(dfg)

	#merge_cols = ['album_artist', 'g', 'adj_pval(BH)']
	dfX = reduce(lambda x, y: pd.merge(x, y, on = 'album_artist'), df_group_by_List)
	dfX = dfX.rename(columns={'g': 'country', 'adj_pval(BH)': 'p_country', 'g_x': 'os', \
							'adj_pval(BH)_x': 'p_os', 'g_y': 'access', 'adj_pval(BH)_y': 'p_access'})

	dfX['p_avg'] = dfX[['p_os', 'p_access', 'p_country']].mean(axis=1)
	dfX.sort_values(by='p_avg')

	return dfX


#there are still a lot of artists with low combined p-values (simultaneously for country access and os)
#find the most_common

collections.Counter(dfX[dfX.p_avg <= 0.01].country).most_common
collections.Counter(dfX[dfX.p_avg <= 0.01].access)
collections.Counter(dfX[dfX.p_avg <= 0.01].os)




#Linux users:
#A lot of artists have exactly
#k = 1400 
#m = 1400
#Examples: #Aurora Cano, Micha Burden, Frederic Goodson
#Mmmmmmmm

























######################
## Helper functions ##
######################

class MyPool:

	def __init__(self, processes=1):
		self.pool = Pool(processes)
		self.results = []
	
	def applyAsync(self, f, args):
		r = self.pool.apply_async(f, args)
		self.results.append(r)
		
	def checkPool(self):
		try:
			for res in self.results:
				res.get(0xFFFFFFFF)
			self.pool.close()
			self.pool.join()
		except KeyboardInterrupt:
			print "Caught KeyboardInterrupt, terminating workers"
			self.pool.terminate()
			self.pool.join()
			sys.exit()


def executeCmd(cmd, jobType = "local"):

	print(cmd)
	print("")
	sys.stdout.flush()
	rc = subprocess.call(cmd, shell=True)
	#rc = 0
	if rc == 1:
		print("Error while processing " + cmd)
		return 1
	else:
		return 0
