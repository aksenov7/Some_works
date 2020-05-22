from crawlers.help_functions import *

def parse_buisness_domain(url, lab, mydir):
    '''This function write domain from all pages in this category
    Parameters: url - url for parse domain
                lab - labels for map domain in current url
                mydir - dir for save files with urls
    result: writes file with domain and lab to dir'''
    
    log = logging.getLogger('scrapError_Buisness')
    
    log.info('Start cat at {}'.format(dt.datetime.now()))
    one_cat = []
    exist = True
    i = 0
    cat = url.split('/')[-1]
    url = url + '?page={}&sort=popular'
    while exist:
        i+=1
        bs = makebs(url.format(i))
        if bs is None:
            log.warning('Bs not create on url - {} in {}'.format(url.format(i), dt.datetime.now()))
            break
        log.warning('Bs create on url - {} in {}'.format(url.format(i), dt.datetime.now()))
     
        for link in bs.findAll('a'):
            if 'href' in link.attrs:
               
                if len(link['href'].split('/'))==3:
                    if link['href'].split('/')[-2] == 'reviews':
                        domain = link['href'].split('/')[-1]
                        if [domain, lab] in one_cat:
                            log.info("Page nimber {} is not exist - {}!".format(i, dt.datetime.now()))
                            exist = False
                            break
                        one_cat.append([domain, lab])
        log.info('Page ends with parse {} sites at {}'.format(len(one_cat),dt.datetime.now()))
        
    log.info('Parse cat ends at {} with {} sites'.format(dt.datetime.now(), len(one_cat)))
    new_df = pd.DataFrame(one_cat, columns=['domain', 'lab'])
    log.info('DF create at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.domain = new_df.domain.apply(normDom)
    log.info('DF normolize at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.to_csv('{}/buisnesspages_{}.csv'.format(mydir, cat), sep='\t', index=False, header=None)
    log.info('Df save at {}'.format(dt.datetime.now()))



def parse_all(df, mydir):
    '''This function realise concurent parsing of pages
    Parameters: df - data frame object with urls for parse and labels
    result: write files with domain and lab to dir'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    future_to_url = {executor.submit(parse_buisness_domain, row[-2], row[-1], mydir): row for row in df.values}
                    for future in concurrent.futures.as_completed(future_to_url):
                        future_to_url[future]
    
