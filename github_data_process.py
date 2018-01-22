# coding: utf-8

# In[3]:


import numpy as np
import pandas as pd
# import seaborn as sns
import matplotlib
matplotlib.use('Agg')
# sns.set()
from matplotlib import pyplot as plt

import datetime


# In[61]:


# def format_save_charts(axis, save_path):
#     fig = plt.figure()
#     axis = axis
#     plt.tight_layout()
#     fig.savefig(save_path)
#
#     return axis


class TokenAnalyzer(object):
    """Cass for analyzing token performance and interest
    
    The idea here is to provide a data class for tokens and generate visualizations based on the class
    
    """

    def __init__(
            self,
            contribution_data,
            repo_dictionary_data,
            price_data,
            end_time=datetime.date.today(),
    ):
        """Class constructor
        
        Args:
            contribution_data: dataframe for github contribution history
            repo_dictionary_data: dataframe for github repositories data
            price_data: dataframe for historical price/ marketcap information of coins
        """
        self.contribution_data = contribution_data.rename(columns={'login': 'developer'})
        self.repo_dictionary_data = repo_dictionary_data.rename(columns={'full_name': 'repo_full_name'})
        self.price_data = price_data
        self.end_time = end_time
        self.this_monday = end_time - datetime.timedelta(days=today.weekday())
        self.last_monday = end_time - datetime.timedelta(days=today.weekday() + 7)
        self.four_weeks_ago = end_time - datetime.timedelta(days=today.weekday() + 28)

        self.process_data()

    def process_data(self):
        """Method to process data for google trend and price index.
        
        Called by constructor. No need to use. Basic data joining
        """
        self.contribution_data.week = pd.to_datetime(self.contribution_data.week)
        self.contribution_data = self.contribution_data[self.contribution_data.week >= datetime.date(2010, 1, 1)]
        self.contribution_data.week = self.contribution_data.week + datetime.timedelta(1)
        self.contribution_data = self.contribution_data.merge(self.repo_dictionary_data[['repo_full_name', 'symbol']],
                                                              how='left')

    def filter_token_contribution(self,
                                  symbols=None,
                                  developers=None,
                                  repo_full_names=None,
                                  end_time=None,
                                  time_range=7):
        """Method to filter contribution data
        
        Args:
            symbols: list type, list of token tickers
            developers: list type, list of github logins
            repo_full_names: list type, list of github repo full name e.g. bitcoin/bitcoin
            end_time: last day of filtering
            time_range: how many days to go back
        
        """
        # determining time range
        if end_time == None:
            end_time = self.this_monday
        else:
            pass
        start_time = end_time - datetime.timedelta(time_range)
        self.contribution_data_filter = self.contribution_data[(self.contribution_data.week >= start_time) &
                                                               (self.contribution_data.week < end_time)]

        # determining token and developer filters
        if symbols <> None:
            self.contribution_data_filter = self.contribution_data_filter[
                self.contribution_data_filter.symbol.isin(symbols)]
        else:
            pass
        if developers <> None:
            self.contribution_data_filter = self.contribution_data_filter[
                self.contribution_data_filter.developer.isin(developers)]
        else:
            pass

        if repo_full_names <> None:
            self.contribution_data_filter = self.contribution_data_filter[
                self.contribution_data_filter.repo_full_name.isin(repo_full_names)]
        else:
            pass

        return self.contribution_data_filter

    def get_commit_leaderboard(self, dimension=['symbol'], limit=20, *args, **kwargs):
        """Method to plot the most commited token of last 
        
        """
        self.contribution_data_filter = self.filter_token_contribution(*args, **kwargs)
        self.commit_leaderboard = self.contribution_data_filter.groupby(dimension).commits.sum().sort_values(
            ascending=False)[0:limit]

        return self.commit_leaderboard


# In[52]:


# main function

# ingesting from csv
today = datetime.date.today()
coin_marketcap_data = pd.DataFrame.from_csv('./data/latest_data/coin_marketcap_data.csv')
coin_github_repo_data = pd.DataFrame.from_csv('./data/latest_data/top_coin_repo_list.csv', index_col=None)
data_contributions_entry = pd.DataFrame.from_csv('./data/latest_data/top_coin_repo_contributions_entry.csv')
data_contributions_entry = data_contributions_entry[
    ~data_contributions_entry.repo_full_name.isin(['input-output-hk/nixpkgs'])]

# In[ ]:


# In[53]:


token_class = TokenAnalyzer(contribution_data=data_contributions_entry,
                            repo_dictionary_data=coin_github_repo_data,
                            price_data=coin_marketcap_data)

# In[64]:


# Generate last week commit 
fig = plt.figure()
token_class.get_commit_leaderboard(dimension=['repo_full_name'], limit=20).sort_values().plot(kind='barh',
                                                                                              title='last_week_top_committed_repo\nAs of: ' + str(
                                                                                                  datetime.date.today()))
plt.xlabel('commits')
plt.ylabel('repo')
plt.tight_layout()
fig.savefig('./data/pictures/last_week_top_committed_repo.png')

# In[66]:


# Generate last week commit 
fig = plt.figure()
token_class.get_commit_leaderboard(dimension=['symbol'], limit=20).sort_values().plot(kind='barh',
                                                                                      title='last_week_top_committed_token\nAs of: ' + str(
                                                                                          datetime.date.today()))
plt.xlabel('commits')
plt.ylabel('token')
plt.tight_layout()
fig.savefig('./data/pictures/last_week_top_committed_token.png')

# In[67]:


# Generate last week commit 
fig = plt.figure()
token_class.get_commit_leaderboard(dimension=['developer'], limit=20).sort_values().plot(kind='barh',
                                                                                         title='last_week_top_committed_developer\nAs of: ' + str(
                                                                                             datetime.date.today()))
plt.xlabel('commits')
plt.ylabel('developer')
plt.tight_layout()
fig.savefig('./data/pictures/last_week_top_committed_developer.png')
