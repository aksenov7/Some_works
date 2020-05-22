from crawlers.help_functions import *

def parse_ru_domain(url, lab, mydir):
    '''This function write domain from all pages in this category
    Parameters: url - url for parse domain
                lab - labels for map domain in current url
                mydir - dir for save files with urls
    result: writes file with domain and lab to dir'''
    
    log = logging.getLogger('scrapError_Russian')
    
    log.info('Start cat at {}'.format(dt.datetime.now()))
    one_cat = []
    i = 0
    try:
        url = re.sub('_1.html', '_{}.html', url)
    except Exception as e:
        log.error("Url: {} - is not correct".format(url))
        return
    while True:
        i+=1
        bs = makebs(url.format(i))
        if bs is None:
            log.warning('Bs not create - {}'.format(dt.datetime.now()))
            break
        log.info('Bs create on url - {} in {}'.format(url.format(i), dt.datetime.now()))
        bs = bs.find('index')
        if bs is None:
            log.warning('Bs not create - {}'.format(dt.datetime.now()))
            break
        try:
            bs.findAll('table')[0].decompose()
        except:
            log.warning('Page number {} is not exist. End parse url at {}!'.format(i, dt.datetime.now()))
            break
        if len(bs.findAll('a', {'class':'lnk'})) == 0:
            log.warning('Page number {} is not exist. End parse url at {}!'.format(i, dt.datetime.now()))
            break
        for link in bs.findAll('a', {'class':'lnk'}):
            if 'href' in link.attrs:
                one_cat.append([link['href'], lab])

        log.info('Page ends with parse {} sites at {}'.format(len(one_cat) ,dt.datetime.now()))
    log.info('Parse cat ends at {} with {} sites'.format(dt.datetime.now(), len(one_cat)))
    new_df = pd.DataFrame(one_cat, columns=['domain', 'lab'])
    log.info('DF create at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.domain = new_df.domain.apply(normDom)
    log.info('DF normolize at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.to_csv('{}/russianpages_{}.csv'.format(mydir, url.split('/')[-3]), sep='\t', index=False, header=None)
    log.info('Df save at {}'.format(dt.datetime.now()))

def parse_all(df, mydir):
    '''This function realise concurent parsing of pages
    Parameters: df - data frame object with urls for parse and labels
    result: write files with domain and lab to dir'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    future_to_url = {executor.submit(parse_ru_domain, row[1], row[2], mydir): row for row in df.values}
                    for future in concurrent.futures.as_completed(future_to_url):
                        future_to_url[future]
    
