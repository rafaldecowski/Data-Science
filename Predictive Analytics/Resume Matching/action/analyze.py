# -*- coding: utf-8 -*-
"""
Created on Tue Nov 21 01:31:45 2017

@author: Rafal
"""
from collections import Counter

import pandas as pd


def extract_mdb_to_pd(collection):
    # Get data from mongodb and convert into dataframe
    col = collection.find({})
    return pd.DataFrame(list(col))


def get_frequencies(df, count):
    ''' Uses Pandas function to group by the feature name, count the frequencies of each value within the feature and sort'''

    features = ['Company', 'Title', 'City', 'State']
    frequency_series = []
    for feature in features:
        frequency_series.append({feature: df.groupby(feature)['_id'].size().sort_values(ascending=False).head(count).to_dict()})

    return frequency_series


def title_vocabulary(df, top_count):

    # Titles Vocabulary contains all words found in titles
    title_vocabulary = []

    # Break apart words in titles
    titles = df['Title'].str.split(' ')

    # Push all words into a single array
    for word_group in titles:
        if word_group:
            for word in word_group:
                title_vocabulary.append(word.strip())

    # Get Word frequency into a dictionary
    title_word_count = dict(Counter(title_vocabulary))

    # Convert into a dataframe, sort and extract top results
    title_df = pd.DataFrame(index=list(title_word_count), data={'count': list(title_word_count.values())}, )
    title_df = title_df[~(title_df.index.isin(['And', 'Usa', 'I', 'Li', '', ' ']))]

    # Exclude searched words - may be used for alternative positions/keywords
    # title_df = title_df[~(title_df.index.isin(['data', 'engineer']))]


    top = title_df.sort_values('count', ascending=False).head(top_count)
    print('Key Words: ' + str(top))

    return title_df


def title(title_df):
    '''
        It determines seniority levels and frequency.
        It returns back the title dictionary without the seniority levels and keywords.
    '''



    # Define title seniority levels and keywords for each
    # The keywords should start with a capital letter as every word within the title was converted during collection
    rank_keywords = {'intern': ['Intern', 'Internship'],
                     'junior': ['Jr', 'Junior', 'Entry', 'Associate', 'New'],
                     'senior': ['Sr', 'Senior', 'Senior '],
                     'lead': ['Lead', 'Chief', 'Principal', 'Head'],
                     'manager': ['Manager'],
                     'director': ['Director', 'Vice', 'President']}

    # For each level, sum the number of matched keywords
    intern = int(title_df[(title_df.index.isin(rank_keywords['intern']))].sum())
    junior = int(title_df[(title_df.index.isin(rank_keywords['junior']))].sum())
    senior = int(title_df[(title_df.index.isin(rank_keywords['senior']))].sum())
    lead = int(title_df[(title_df.index.isin(rank_keywords['lead']))].sum())
    manager = int(title_df[(title_df.index.isin(rank_keywords['manager']))].sum())
    director = int(title_df[(title_df.index.isin(rank_keywords['director']))].sum())

    # Convert the seniority into a dictionary for charting
    seniority_dict = {'Intern': intern,
                      'Junior': junior,
                      'Senior': senior,
                      'Lead': lead,
                      'Manager': manager,
                      'Director': director}


    print(seniority_dict)

    # Remove ranks from top titles
    title_df_no_rank = title_df[(~title_df.index.isin(rank_keywords['junior']))]
    title_df_no_rank = title_df_no_rank[~(title_df_no_rank.index.isin(rank_keywords['senior']))]
    title_df_no_rank = title_df_no_rank[~(title_df_no_rank.index.isin(rank_keywords['lead']))]
    title_df_no_rank = title_df_no_rank[~(title_df_no_rank.index.isin(rank_keywords['manager']))]
    title_df_no_rank = title_df_no_rank[~(title_df_no_rank.index.isin(rank_keywords['director']))]


    # Remove insignificant words
    #title_df_no_rank = title_df_no_rank[~(title_df_no_rank.index.isin(['And', 'Usa', ' ']))]
    title_df_no_rank = title_df_no_rank.sort_values('count')

    # @TODO the mongodb inser should be triggerted from a separate function
    # Insert the summarized data into mongodb
    #analyzed_collection = str(collection.name) + '_analyzed'
    #analyzed_collection.insert_many(json.loads(title_df_no_rank.T.to_json()).values())


def position_type(title_df):

    type_keywords = {'full': ['Full', 'Time', 'Permanent'],
                     'part': ['Part', 'Diem'],
                     'temp': ['Temp', 'Temporary'],
                     'cont': ['Contract', 'Cont', 'Contr']}


    # For each level, sum the number of matched keywords
    full = int(title_df[(title_df.index.isin(type_keywords['full']))].sum())
    part = int(title_df[(title_df.index.isin(type_keywords['part']))].sum())
    temp = int(title_df[(title_df.index.isin(type_keywords['temp']))].sum())
    cont = int(title_df[(title_df.index.isin(type_keywords['cont']))].sum())

    # Convert the seniority into a dictionary for charting
    type_dict = {'Full': full,
                 'Part': part,
                 'Temp': temp,
                 'Contract': cont}
    print(type_dict)
    return type_dict



def salary(df, title_df):

    # Get only Salary Columns
    salary_df = df[['Salary Min', 'Salary Max']]

    # Convert it to numeric
    salary_df = salary_df[['Salary Min', 'Salary Max']].apply(pd.to_numeric)

    # Drop any rows with salary of 0
    salary_df = salary_df[salary_df['Salary Min'] > 0]
    salary_df = salary_df[salary_df['Salary Max'] > 0]

    # Create a new column - salary average
    salary_df['avg'] = (salary_df['Salary Max'] + salary_df['Salary Min']) / 2


    salary_df['Title'] = df['Title']

    # linear regression on Salary range
    #sns.regplot(x="Salary Max", y="Salary Min", data=salary_df)


    # Get salary range for top 20 keywords
    keywords = title_df.sort_values(by='count', ascending=False)
    keywords = keywords[keywords.index != '-'].head(20).index

    kw_worth = []

    for kw in keywords:

        kw_worth.append({kw: list(salary_df[salary_df['Title'].str.contains('Data')]['avg'].astype(float))})


    kw_worth_df = pd.DataFrame(columns=kw_worth.keys(), data=kw_worth.values())
