#! /usr/bin/python

import pandas as pd
import ast

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

def nfsc():

	#Get number of streams per user:
	#streams here is a track played by a user
	#stream counts is the number of times a track is played by a user
	stream_counts = streams.groupby(['user_id', 'track_id']).size().reset_index(name='counts')
	stream_counts.sort_values(by='counts', ascending=False)

	#Now sorting the user accounts by number of streams doesn't really prove anything
	#But some insights can be acquired 

	#If I look at the users in top of stream_counts 
	users[users.user_id == '8ec3584b7ef681f6b817c6b2a5b54153011d7efa']
	#I see most of them are from country 59
	#Let's check the distribution of country of origin for the top 1000 users in stream_counts:
	top1000_streams_users = stream_counts.sort_values(by='counts', ascending=False).head(1000).user_id
	#top1000_streams_users_countries = users[users.user_id.isin(top1000_streams_users)]['country']
	top1000_streams_users_countries = [users[users.user_id == u].country.values[0] for u in top1000_streams_users.values]

	plt.figure(figsize=(20,12))
	sb.distplot(top1000_streams_users_countries, bins=users.country.max(), kde=False)
	plt.xlim(0,users.country.max())
	plt.title('Country of origin for the Top 1000 most played tracks/user')
	plt.show()

	#The User accounts responsible for the top100 stream_counts come mainly from a couple of countries. Interesting


	#But maybe there are real human users from one country that are just very heavy users. Yes
	#One could normalize the frequency in the top1000 to the frequency in a background random 1000 streams


	rand1000_streams_users_countries = [users[users.user_id == u].country.values[0] for u in streams.sample(1000).user_id]

	plt.figure(figsize=(20,12))
	sb.distplot(rand1000_streams_users_countries, bins=users.country.max(), kde=False)
	plt.xlim(0,users.country.max())
	plt.title('Country of origin for a random sample of 1000 streams ')
	plt.show()


	#Normalize frequency in top1000 to freq in rand1000
	import collections
	freq_top1000 = collections.Counter(top1000_streams_users_countries)
	freq_rand1000 = collections.Counter(rand1000_streams_users_countries)

	#my parameter can be called normalized frequency of streams per country nfsc
	nfsc = {}
	#if nfsc > 1 :  the country is 'over-represented' in the top1000 streams compared to the background rand1000
	#if nfsc == 1:  equal frequency in the top1000 and rand1000
	#if nfsc < 1 :  the frequency of the country in the top1000 streams is more or less random

	for i in range(1,61):
		f_top   = freq_top1000[i] /1000.0
		f_rand  = freq_rand1000[i]/1000.0
		try:
			nfsc[i] = f_top / f_rand
		except ZeroDivisionError:
			nfsc[i] = 1.0

	return nfsc




'''
#stream_counts.head(1)

#Track 25b1e75619a00b47119dbdd88bf4a37b854eedba is present 2339 times in streams
#out of those 2339 times, user bbc91687167393872dcb557f6413e4acfed3ba77 has streamed it 2333 times.

#In total that track is present 2339 times in streams
len(streams[streams.track_id == '25b1e75619a00b47119dbdd88bf4a37b854eedba'])

#2333 of those times were streamed by just one user. In total only 3 users have streamed it
set(streams[streams.track_id == '25b1e75619a00b47119dbdd88bf4a37b854eedba'].user_id) 
#All 3 of them are from country 59
'''




#CHECK distribution of stream lengths
#see if there is a peak at 30 sec
sb.distplot(streams.length.values)




















#PRF 2: (Potential Red Flag) Track-User Enrichment Analysis
'''
This type of analysis links users with tracks (or albums). The goal is to see whether a track is over-represented in the streams
by one particular user compared to the background (times the track is played by all other users)

for every user build a table like:

				#streams track1 by user		#streams total by user		#streams track1 total		#streams in total
track1					x 							k 							m                       N
annotation2


N: Total number of streams
k: Number of streams by one particular user
m: Number of streams of one particular track (or album)
x: Number of streams of the one particular track (or album) by the one particular user


The hypergeometric test (Fisher's exact test) provides the probability that the track has been played by the user x times by chance
So the lower the p-value the more likely that the user's streams are 'enriched' in that particular track/album.


This will result in a list of p-values, one per track, associated to each user. Get the minimum value.

What about users with very 'niche' taste. Only them (and nobody else) play some tracks (maybe lots of times)


'''




def enrich(document_term_matrix, doc_idxs, n_terms_in_corpus, top_N):
    '''
    uses scipy.stats hypergeometric test to extract probabilities (p-values) of term enrichment in a group of documents
    groups can be defined for instance from the K-Means analysis
    uses absolute count frequencies not tfidf (tfidf are already document-normalized)
    '''

    DTM = document_term_matrix
    enrichment = pd.DataFrame(columns=["word", "word_count_in_cluster", "n_words_in_cluster", "word_count_in_corpus", "n_words_in_corpus", "p-val" ])
    word_idx = DTM[doc_idxs].nonzero()[1]
    word_idx = np.unique(word_idx)
    for i in range(len(word_idx)):
        t = word_idx[i]
        n_terms_in_cluster = len(DTM[doc_idxs].nonzero()[1])
        term_count_in_cluster = DTM[doc_idxs,t].sum()
        term_count_in_corpus = DTM[:,t].sum()
        p = stats.hypergeom.sf(term_count_in_cluster, n_terms_in_corpus, n_terms_in_cluster, term_count_in_corpus)
        if term_count_in_cluster > 20:
            enrichment.loc[i] = [vocab[t], term_count_in_cluster, n_terms_in_cluster, term_count_in_corpus, n_terms_in_corpus, p]
    #Multiple hypothesis correction, transform p-values to adjusted p-values:
    reject, adj_pvalues, corrected_a_sidak, corrected_a_bonf =  mc.multipletests(enrichment["p-val"], method='fdr_bh')
    enrichment["adj_pval(BH)"] = adj_pvalues
    enrichment = enrichment.sort_values(by='adj_pval(BH)').head(top_N)
    return enrichment

#document indexes of cluster 1
cl0_doc_idxs = [c[0] for c in enumerate(clusters) if c[1] == 0]
#Remember N is the total nonzero count values in corpus
#It can be estimated as well as: len(DTM.nonzero()[1])

cluster1_enrichment = enrich(DTM, cl0_doc_idxs, N, 20)










#Potential Red Flags

#In streams, group by track_id (and user_id in second level!)
#see tracks repeated more than a n times within a Delta timestamp (by same user)

#Distribution of the number of streams per user. Has to be normal

#For a user see if the tracks they stream are equally distributed. 
#E.g. a user has 1000 streams and it's exactly 4 songs 250 times each. -> giveaway


#For a user see if the tracks they stream have the exact same time stamp