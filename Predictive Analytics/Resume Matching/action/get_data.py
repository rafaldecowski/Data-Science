import json
import random
import re
from timeit import default_timer as timer
from urllib.request import Request, urlopen

import pandas as pd
import requests
from bs4 import BeautifulSoup


def indeed_postings(indeed_col, title):

    # Array to store all individual job offers
    job_posts = []
    title = title.replace(' ', '+')

    # To loop through a thousand job postings
    # @TODO Find out a better way to loop through the pages
    # There are 15 postings per page but the pagination increses by 10's - why?
    for i in range(0, 1010, 10):

        # TODO update his link to incorporate user input as job titles
        # This is currently setup for data scientist only
        url = 'https://www.indeed.com/jobs?q=' + title + '&start=' + str(i)

        # Use BeautifulSoup to scrape the url
        page = requests.get(url)
        pagehtml = page.text
        soup = BeautifulSoup(pagehtml, 'html.parser')

        # TODO grab stats on the total number of returned results. Compare to the end results count()
        # total_postings = soup.find('div', attrs={'id': 'searchCount'}).text.strip()

        # Extract all post DIVs
        all_post_divs = soup.findAll('div', attrs={'class': 'result'})

        # Loop through all postings on this page and extract specific information
        for post in all_post_divs:

            # Look for title and extract the title attribute value if found
            title = post.find('a', attrs={'data-tn-element': 'jobTitle'})
            if title:
                title = title['title']

            # Look for the brief description/summary of the post and extract as text if found
            description = post.find('span', attrs={'class': 'summary'})
            if description:
                description = description.text.strip()

            # Look for the location of the post and extract as text if found
            location = post.find('span', attrs={'class': 'location'})
            if location:
                location = location.text.strip()

            # Look for the company name of the post and extract as text if found
            company = post.find('span', attrs={'class': 'company'})
            if company:
                company = company.text.strip()

            # Capture link to the actual post for further processing
            link = post.find('a', attrs={'class': 'turnstileLink'})
            if link:
                link = 'https://www.indeed.com/' + str(link['href'])

            # Add this post to the main array with all postings
            job_posts.append({'Title': title,
                              'Description': description,
                              'Location': location,
                              'Company': company,
                              'Link': link})

    # Convert the array to pandas dataframe
    df = pd.DataFrame(list(job_posts))

    # Remove duplicate records
    # Fails when dataframe empty
    dedup = df.drop_duplicates().reset_index()

    # Convert the results from pandas DF to json and push into 'indeed' MDB collection
    indeed_col.insert_many(json.loads(dedup.T.to_json()).values())


def glassdoor_postings(col, title):

    base_url = 'https://www.glassdoor.com'
    page_offers = []

    # Modify the title to match the expected glassdoor search format
    search_title = title.replace(' ', '+')
    search_title = search_title.lower()

    search_url = base_url + '/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword=&sc.keyword=' + search_title + '&locT=&locId=&jobType='
    #url = 'https://www.glassdoor.com/Job/' + title + '-jobs-SRCH_KO0,14_IP.htm'

    url_request = Request(search_url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(url_request).read()
    soup = BeautifulSoup(webpage, 'html.parser')

    # Total timer
    all_start = timer()

    # Process only up to 100 pages
    try:
        pages_found = soup.find('div', attrs={'class': 'cell middle hideMob padVertSm'}).text
        pages_found = int(re.match('.*?([0-9]+)$', pages_found).group(1))
        print('Found pages: ' + str(pages_found))

    except:
        pass

    next_page_url = search_url
    next_page_url = soup.find('li', attrs={'class': 'next'}).a['href']



    # Process only 10 randomly selected pages pages
    if int(pages_found) > 10:
        page_sample = random.sample(range(1, pages_found), 10)
    else:
        page_sample = int(pages_found)

    print('Processsing through the following pages: ' + str(page_sample))
    for i in page_sample:  # page_sample:

        # Single page timers
        #print('Processing page: ' + str(i))
        #s = timer()

        # Page Content extract with BS4
        url = next_page_url.split('.')[0]
        url = base_url + url + '.htm'


        #url = 'https://www.glassdoor.com/Job/' + title + '-jobs-SRCH_KO0,14_IP' + str(i) + '.htm'
        url_request = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(url_request).read()
        soup = BeautifulSoup(webpage, 'html.parser')


        # Extract the list of offers
        all_post_div = soup.findAll('li', attrs={'class': 'jl'})


        # Loop through all posting and collect information for individual offers
        for post in all_post_div:

            # Extract Title and confirm it was found
            info = post.find('i', attrs={'class': 'info'})

            if info:
                # TODO include dash
                title = info['data-jobtitle'].strip().title()  # remove ws and change to 'title' case
                title = re.sub('[^A-Za-z ]+', '', title)  # removing noise from titles

                salary_min = info['data-displayed-min-salary']
                salary_max = info['data-displayed-max-salary']
            else:
                title = ""
                salary_min = 0.00
                salary_max = 0.00

            # Location
            emp_loc = post.find('div', attrs={'class': 'empLoc'}).text
            emp_loc = emp_loc.split(' – ')
            location = emp_loc[1].strip()

            # Convert Location field into City and State
            try:
                city = location.split(',')[0].title()
                state = location.split(',')[1]
                state = state[:3].strip()
            except:
                state = location
                city = ''


            # Company name
            company = emp_loc[0].strip()

            # link to the entire posting description
            link = 'https://www.glassdoor.com/' + post.a['href']


            # Add  collected info into a dictionary
            page_offers.append({'Title': title,
                                # 'Description': description,
                                'City': city,
                                'State': state,
                                'Company': company,
                                'Salary Min': salary_min,
                                'Salary Max': salary_max,
                                'Link': link})
        # End Page timer
        #e = timer()
        #print('Time: ' + str(round(e - s, 2)))


    # Convert the array with all postings into a Pandas Dataframe and Remove duplicate rows
    df = pd.DataFrame(list(page_offers))


    dedup = df.drop_duplicates()

    # Inser the collected data into the mongoDB collection
    col.insert_many(json.loads(dedup.T.to_json()).values())

    # End timer for the entire collection process and print
    print('Collection process for was completed successfully.')
    all_end = timer()
    print('The total time to process ' + str(len(df)) + ' jobs was:' + str(round(all_end - all_start, 2)) + ' sec.')


