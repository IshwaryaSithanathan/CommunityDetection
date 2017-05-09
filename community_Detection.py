# -*- coding: utf-8 -*-
"""
Created on Sat Mar  4 21:20:58 2017

@author: ishwarya
"""
import networkx as nx
import json
import community
import matplotlib.pyplot as plt
from  pymongo import MongoClient
from networkx.readwrite import json_graph
import operator
from collections import defaultdict
import csv

for i in range(0,24):
    tweetsSet = set()
    G = nx.Graph()
    DG = nx.DiGraph()
    Q = {}
    
    start='Mon Feb 27 '+str(i).zfill(2)+':00:00 +0000 2017'
    end='Mon Feb 27 '+str(i).zfill(2)+':29:59 +0000 2017'
    connection = MongoClient('localhost', 27017)
    db = connection.twitter
    tweets = list(db.Nokia.find({'created_at':{'$gte':start,'$lte':end }}))
#    with open('Nokia_Tweets_Count_Statistics.txt', 'a') as tf:
#        tf.write(start+" : "+str(len(tweets))+'\n') 
#        tf.close()
    for tweet in tweets:
        # avoid clones
        if tweet['id_str'] not in tweetsSet:
          tweetsSet.add(tweet['id_str'])
          # if the tweet is a retweet add a link (user, original user)
          if 'retweeted_status' in tweet:
            G.add_edge(tweet['user']['id_str'],tweet['retweeted_status']['user']['id_str'])
            DG.add_edge(tweet['user']['id_str'],tweet['retweeted_status']['user']['id_str'])
            Q[(tweet['user']['id_str'],tweet['retweeted_status']['user']['id_str'])] = Q.get((tweet['user']['id_str'],tweet['retweeted_status']['user']['id_str']), 0.) + 1
          # if the tweet contains mentions add a link (user, mentioned user)
          # only in the case user and mentioned user are not equal
          if tweet['entities']['user_mentions']:
            for user in tweet['entities']['user_mentions']:
              #if userA RT userB, twitter API return that userA mentions userB. Weird.
              if (user != tweet['user']):
                G.add_edge(tweet['user']['id_str'],user['id_str'])
                DG.add_edge(tweet['user']['id_str'],user['id_str'])
    #            Q[(tweet['user']['id_str'],user)] = Q.get((tweet['user']['id_str'],user['id_str']), 0.) + 1
    if tweets:
#        with open('Networkdata_1_'+str(i)+'.json', 'w') as outfile:
#                outfile.write(json.dumps(json_graph.node_link_data(G)))
        part = community.best_partition(G)
        values = [part.get(node) for node in G.nodes()]
        count=list(set([i for i in values]))
        mod = community.modularity(part,G)
        communities = defaultdict(list)
        for key, value in sorted(part.items()):
            communities[value].append(key)
        sortedComm=sorted(communities.values(), key=len, reverse=True)[:5]
        with open('./Communities/Nokia_TopCommunities_27_'+str(i)+'.csv','a') as f:
            w = csv.writer(f)
            w.writerow(['Size','Users'])
            for ele in sortedComm:
                w.writerow([len(ele),ele])
        with open('./Communities/Nokia_Communities_1_'+str(i)+'.csv','a') as f:
            w = csv.writer(f)
            w.writerow(['Communities_Id','Users'])
            for key, value in communities.items():
                w.writerow([key, value])
        degree = nx.degree_centrality(DG)
        with open('./Degree/Nokia_Degree_1_'+str(i)+'.csv','a') as f:
            w = csv.writer(f)
            w.writerow(['Users','Degree'])
            for key, value in degree.items():
                w.writerow([key, value])
        page_rank = nx.pagerank(DG)
        node = max(page_rank.items(), key=operator.itemgetter(1))[0]
        for tweet1 in tweets :
            if tweet1['user']['id_str'] == node or tweet1['id_str'] == node or ('retweeted_status' in tweet1 and tweet1['retweeted_status']['user']['id_str'] == node):
                    user = tweet1['user']
                    with open('./KeyPlayer/Nokia_KeyPlayer_1_'+str(i)+'.json', 'w') as outfile3:
                        outfile3.write(json.dumps(user))
        with open('Nokia_Graph_Statistics.txt', 'a') as tf:
            tf.write(start+": Number of Communities - "+str(len(count))+", Modularity score - "+str(mod)+'\n') 
            tf.close()
        plt.figure(i)
        nx.draw_spring(G, cmap = plt.get_cmap('jet'), node_color = values, node_size=30, with_labels=False)
