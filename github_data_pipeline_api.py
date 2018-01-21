# coding: utf-8

# In[2]:


import datetime
import json
import warnings

import pandas as pd
import requests
from pandas.io.json import json_normalize

warnings.filterwarnings('ignore')
import sys

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('./utils')
from google_sheet_update import *


# In[3]:


# utility function, http request
def getjson(url, auth=0):
    if auth == 0:
        with open('./utils/secret.json') as json_file:
            secret = json.load(json_file)
        auth = (str(secret['github']['username']), str(secret['github']['password']))
    else:
        pass
    header = {'x-requested-with': 'XMLHttpRequest'}
    mainPage = requests.get(url, auth=auth)
    data = mainPage.json()
    return data


# In[4]:


# ingest coin github org
def coin_github_org_ingestion():
    sheet_key = '1tpOAiuRo9RNKnyPCVTGjc3H9S1miIJD1AimFLg8sv4E'
    tab = 'organization'
    data = get_googlesheet_data(sheet_key, tab)
    return data


# ingest coin github exclude data
def coin_github_exclusion_ingestion():
    sheet_key = '1tpOAiuRo9RNKnyPCVTGjc3H9S1miIJD1AimFLg8sv4E'
    tab = 'excluding_repos'
    data = get_googlesheet_data(sheet_key, tab)
    return data


# In[5]:


# ingest coinmarketcap data
def coin_marketcap_ingestion(limit=200):
    data = json_normalize(getjson("https://api.coinmarketcap.com/v1/ticker/?limit=" + str(limit)))
    return data


# In[6]:


# ingest github repo data
def github_repo_ingestion(github_org_data, trunc_date=datetime.date(2017, 1, 1)):
    start_time = datetime.datetime.today()
    data = pd.DataFrame()
    for symbol in github_org_data.symbol.unique():
        for github_org in list(github_org_data[github_org_data.symbol == symbol].github_org):
            try:
                data_repo = getjson("https://api.github.com/users/" + str(
                    github_org) + "/repos?sort=updated&direction=desc&per_page=100")
                repo_dict = json_normalize(data_repo).set_index('name')
                repo_dict['updated_at'] = pd.to_datetime(repo_dict['updated_at'])
                repo_dict['symbol'] = symbol
                repo_list = repo_dict[repo_dict.updated_at >= trunc_date].index
                data = data.append(repo_dict)
                print str(github_org) + ' completed!'
            except:
                print str(github_org) + ' failed!'
                pass

    #     pd.DataFrame.to_csv(token_repo_df,'./data/token_repo_dictionary_'+str(today)+'.csv')
    minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
    data.pushed_at = pd.to_datetime(data.pushed_at)
    print 'finished ingesting coin github repo data! used ' + str(minutes_passed) + ' minutes!'
    return data


# In[7]:


# write github org google sheet with coins that needs to be updated with github orgs
def update_no_org_coins(coin_github_org_data, coin_marketcap_data):
    coin_org_list = coin_github_org_data.symbol.unique()
    coin_total_list = coin_marketcap_data.symbol.unique()
    coin_gap_list = list(set(coin_total_list) - set(coin_org_list))
    coin_gap_list_df = coin_marketcap_data[coin_marketcap_data.symbol.isin(coin_gap_list)][['symbol', 'id']]
    sheet_key = '1tpOAiuRo9RNKnyPCVTGjc3H9S1miIJD1AimFLg8sv4E'
    tab = 'undocumented_top_200_coins'
    cell_col = 'A'
    cell_row = 1
    write_cells(coin_gap_list_df, sheet_key, tab, cell_col, cell_row, transpose=0)
    return coin_gap_list


# In[8]:


# full contribution list per repo
def get_full_contribution_history(coin_github_repo_data):
    start_time = datetime.datetime.today()
    data_contributions_entry = pd.DataFrame()

    for repo_name in coin_github_repo_data.full_name.unique():
        try:
            data_repo_contributors = json_normalize(getjson(
                "https://api.github.com/repos/" + repo_name + "/stats/contributors?sort=total&direction=desc&per_page=100"))
            data_repo_contributors['repo_full_name'] = repo_name
            data_repo_contributors = \
                data_repo_contributors.dropna(subset=['author.login']).set_index(['repo_full_name', 'author.login'])[
                    ['weeks']]
            data_repo_contributors = data_repo_contributors.weeks.apply(pd.Series)
            data_repo_contributors = pd.DataFrame(data_repo_contributors.stack())[0].apply(pd.Series)
            data_repo_contributors = data_repo_contributors[data_repo_contributors.c > 0]
            data_contributions_entry = data_contributions_entry.append(data_repo_contributors)
            memory = (data_contributions_entry.memory_usage()).sum() / (1024 ** 2)
            minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
            print 'repo ' + repo_name + ' flattern completed! used ' + str(
                minutes_passed) + ' minutes! ' + 'memory used ' + str(memory) + 'MB'
            del data_repo_contributors

        except:
            print 'repo ' + repo_name + ' flattern failed! used ' + str(
                minutes_passed) + ' minutes! ' + 'memory used ' + str(memory) + 'MB'
            pass

    minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
    print 'finished ingesting coin contribution data! used ' + str(minutes_passed) + ' minutes!'

    data_contributions_entry['w'] = pd.to_datetime(data_contributions_entry.w, unit='s')
    data_contributions_entry = data_contributions_entry.reset_index().drop(['level_2'], axis=1)
    data_contributions_entry = data_contributions_entry.rename(
        columns={'w': 'week', 'c': 'commits', 'a': 'additions', 'd': 'deletions', 'author.login': 'login'})
    return data_contributions_entry


# In[52]:


# pulling repo lists that need to be updated
def generate_update_repo_list(data_contributions_entry_existing, coin_github_repo_data):
    # dropping empty rows
    data_contributions_entry_existing = data_contributions_entry_existing[data_contributions_entry_existing.commits > 0]
    # formatting dates
    data_contributions_entry_existing.week = pd.to_datetime(data_contributions_entry_existing.week)
    coin_github_repo_data.pushed_at = pd.to_datetime(coin_github_repo_data.pushed_at)
    # contribution update_time
    contribution_update_time = data_contributions_entry_existing.week.max()
    # existing records for last commit week
    repo_last_commit_week = pd.DataFrame(
        data_contributions_entry_existing.groupby('repo_full_name').week.max()).reset_index()
    # latest last commit timestamp from github repo
    repo_latest_record_week = coin_github_repo_data[['full_name', 'pushed_at']].rename(
        columns={'full_name': 'repo_full_name'})
    # merge to generate list of repo lists that have a new push
    repo_compare = repo_last_commit_week.merge(repo_latest_record_week, how='right')
    repo_compare.week = pd.to_datetime(repo_compare.week).fillna(datetime.datetime(1900, 1, 1))
    repo_update_list = repo_compare[((repo_compare.pushed_at - repo_compare.week).dt.days > 7) &
                                    (repo_compare.pushed_at > contribution_update_time - datetime.timedelta(
                                        7))].repo_full_name
    return repo_update_list


# In[ ]:


# In[23]:


# full contribution list per repo
def update_contribution_history(data_contributions_entry_existing, coin_github_repo_data):
    # generate repo lists that needs to be updated

    repo_update_list = generate_update_repo_list(data_contributions_entry_existing, coin_github_repo_data)
    print 'number of repos needed to be updated: ' + str(len(repo_update_list))

    start_time = datetime.datetime.today()
    data_contributions_entry = pd.DataFrame()

    for repo_name in repo_update_list:
        try:
            data_repo_contributors = json_normalize(getjson(
                "https://api.github.com/repos/" + repo_name + "/stats/contributors?sort=total&direction=desc&per_page=100"))
            data_repo_contributors['repo_full_name'] = repo_name
            data_repo_contributors = \
                data_repo_contributors.dropna(subset=['author.login']).set_index(['repo_full_name', 'author.login'])[
                    ['weeks']]
            data_repo_contributors = data_repo_contributors.weeks.apply(pd.Series)
            data_repo_contributors = pd.DataFrame(data_repo_contributors.stack())[0].apply(pd.Series)
            data_repo_contributors = data_repo_contributors[data_repo_contributors.c > 0]
            data_contributions_entry = data_contributions_entry.append(data_repo_contributors)
            memory = (data_contributions_entry.memory_usage()).sum() / (1024 ** 2)
            minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
            print 'repo ' + repo_name + ' flattern completed! used ' + str(
                minutes_passed) + ' minutes! ' + 'memory used ' + str(memory) + 'MB'
            del data_repo_contributors

        except:
            print 'repo ' + repo_name + ' flattern failed! used ' + str(
                minutes_passed) + ' minutes! ' + 'memory used ' + str(memory) + 'MB'
            pass

    minutes_passed = (datetime.datetime.today() - start_time).seconds / 60
    print 'finished ingesting coin contribution data! used ' + str(minutes_passed) + ' minutes!'

    data_contributions_entry['w'] = pd.to_datetime(data_contributions_entry.w, unit='s')
    data_contributions_entry = data_contributions_entry.reset_index().drop(['level_2'], axis=1)
    data_contributions_entry = data_contributions_entry.rename(
        columns={'w': 'week', 'c': 'commits', 'a': 'additions', 'd': 'deletions', 'author.login': 'login'})

    data_contributions_entry_updated = data_contributions_entry_existing[
        (~data_contributions_entry_existing.repo_full_name.isin(repo_update_list)) &
        (data_contributions_entry_existing.commits > 0)].append(data_contributions_entry)
    data_contributions_entry_updated.week = pd.to_datetime(data_contributions_entry_updated.week)
    data_contributions_entry_updated = data_contributions_entry_updated[
        data_contributions_entry_updated.week >= datetime.date(2009, 1, 1)]
    return data_contributions_entry_updated


# In[11]:


# main function, update

coin_github_org_data = coin_github_org_ingestion()
coin_marketcap_data = coin_marketcap_ingestion()
coin_github_repo_data = github_repo_ingestion(coin_github_org_data)
coin_github_exclude_data = coin_github_exclusion_ingestion()
coin_gap_list = update_no_org_coins(coin_github_org_data, coin_marketcap_data)

# update contribution data from existing file
data_contributions_entry_existing = pd.DataFrame.from_csv('./data/latest_data/top_coin_repo_contributions_entry.csv')
data_contributions_entry = update_contribution_history(data_contributions_entry_existing, coin_github_repo_data)
data_contributions_entry = data_contributions_entry[~data_contributions_entry.repo_full_name.isin(coin_github_exclude_data.repo_full_name)]
# pull from scratch
# data_contributions_entry = get_full_contribution_history(coin_github_repo_data)


# In[69]:


# saving to csv
today = datetime.date.today()
pd.DataFrame.to_csv(coin_marketcap_data, './data/latest_data/coin_marketcap_data.csv')
pd.DataFrame.to_csv(coin_github_repo_data, './data/latest_data//top_coin_repo_list.csv')
pd.DataFrame.to_csv(data_contributions_entry, './data/latest_data/top_coin_repo_contributions_entry.csv')
# archiving just token contribution data
pd.DataFrame.to_csv(data_contributions_entry,
                    './data/archive_data/top_coin_repo_contributions_entry_' + str(today) + '.csv')

print 'finished github_data_pipeline! UTC time:'+str(datetime.datetime.today())