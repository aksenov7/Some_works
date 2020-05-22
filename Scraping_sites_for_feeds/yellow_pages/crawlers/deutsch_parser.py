from crawlers.help_functions import *

def parse_domain(df, key, mydir):
    '''This function write domain from all pages in this category
    Parameters: df - data frame object with urls for parse and labels
                key - category for filtr urls from df
                mydir - dir for save files with domains
    result: writes file with domain and lab to dir'''
    
    log = logging.getLogger('scrapError_deutsch')
    
    log.info('Start cat at {}'.format(dt.datetime.now()))
    one_cat = []
    lab = df[df.cat==key].lab.values[0]
    for url in df[df.cat==key].url.values:
        i = 0
        url = url[:-1]
        while True:
            i+=1
            bs = makebs(url+str(i))
            if bs is None:
                log.warning('Bs not create - {}'.format(dt.datetime.now()))
                break
            bs = bs.find('div', {'id':'gs_treffer'})
            if bs is None:
                log.warning('Not content on page - {}'.format(dt.datetime.now()))
                break
            if bs.find('article') is None: break
            websites = bs.findAll('a', {'rel':'follow noopener'})
            for website in websites:
                if 'href' in website.attrs:
                    one_cat.append([website['href'], lab])
            log.info('Page ends with parse {} sites at {}'.format(len(one_cat) ,dt.datetime.now()))

    log.info('Parse cat ends at {} with {} sites'.format(dt.datetime.now(), len(one_cat)))
    new_df = pd.DataFrame(one_cat, columns=['domain', 'lab'])
    log.info('DF create at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.domain = new_df.domain.apply(normDom)
    log.info('DF normolize at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.to_csv('{}/deutchepages_{}.csv'.format(mydir, key), sep='\t', index=False, header=None)
    log.info('Df save at {}'.format(dt.datetime.now()))

def parse_all(df, mydir):
    '''This function realise concurent parsing of pages
    Parameters: df - data frame object with urls for parse and labels
    result: write files with domain and lab to dir'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    future_to_url = {executor.submit(parse_domain, df, key, mydir): key for key in df.cat.unique()}
                    for future in concurrent.futures.as_completed(future_to_url):
                        future_to_url[future]

