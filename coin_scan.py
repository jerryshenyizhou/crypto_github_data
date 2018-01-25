import time
import warnings

import pandas as pd
from coinmarketcap import Market
from lxml import html

warnings.filterwarnings('ignore')

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_rt_coin():
    # get realtime quotes
    coinmarketcap = Market()
    list_rt_quotes = coinmarketcap.ticker(limit=2000)
    df_rt_quotes = pd.DataFrame(list_rt_quotes)
    return df_rt_quotes


def get_token_history(token_id):
    # error handling and timeout of requests stolen from:
    # https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    t0 = time.time()
    try:
        page = requests_retry_session().get(
            'https://coinmarketcap.com/currencies/%s/historical-data/' % token_id,
            timeout=5
        )
    except Exception as x:
        print('It failed :(', x.__class__.__name__)
    else:
        print('It eventually worked', page.status_code)
    finally:
        t1 = time.time()
        print('Took', t1 - t0, 'seconds')

    df_current = pd.DataFrame()

    try:
        html_content = html.fromstring(page.content)
        table_list = html_content.xpath('//*[@id="historical-data"]/div/div[3]/table')

        list_record = [item.strip().replace(',', '').replace('-', '') for item in
                       table_list[0][1].text_content().split('\n') if bool(item.split())]

        n_row = len(list_record) / 7

        list_rows = [list_record[i * 7:i * 7 + 7] for i in range(n_row)]

        df_current = pd.DataFrame(list_rows, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'marketcap'])
        df_current['date'] = pd.to_datetime(df_current.date)
        df_current.set_index('date', inplace=True)
        # convert all values to float
        df_current = df_current.apply(pd.to_numeric)

        df_current['volume_token'] = df_current.volume / df_current.close
        df_current['id'] = token_id
    except Exception:
        print 'Scraping failed'
    return df_current


time_start = time.time()
df_rt = get_rt_coin()
df_all = pd.DataFrame()
for token_id in df_rt.id.head(300).values:
    print token_id
    df_current = get_token_history(token_id)
    df_all = df_all.append(df_current)

df_all.to_csv('all_data.csv')
print 'total time used: %d s'%(time.time()-time_start)