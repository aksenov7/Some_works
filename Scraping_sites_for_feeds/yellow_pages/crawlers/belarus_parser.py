from crawlers.help_functions import *

def temporary_write(url, lab, mydir):
    '''This function write domain from all pages in this category
    Parameters: url - url for parse domain
                lab - labels for map domain in current url
                mydir - dir for save files with urls
    result: writes file with domains and lab to dir'''

    log = logging.getLogger('scrapError_Belarus')
    
    url = url + '?page={}'
    log.info('Start category {} at {}!'.format(url.split('/')[7], dt.datetime.now()))
    domain_list = page_itteration(url, log)
    log.info('End category {} at {}!'.format(url.split('/')[7], dt.datetime.now()))
    df = pd.DataFrame(domain_list, columns=['domain'])
    df.drop_duplicates(inplace=True)
    log.info('Make df on category {} with shape {} at {}!'.format(url.split('/')[7], df.shape, dt.datetime.now()))
    df['lab'] = lab
    df.to_csv('{}/{}.csv'.format(mydir, url.split('/')[7]),
              sep='\t', index=False, header=None)
    log.info('Save df on category {} at {}!'.format(url.split('/')[7], dt.datetime.now()))

def page_itteration(url, log):
    '''This function define number of pages in category and append domains from all pages in one list
    Parameters: url - url for parse category
                log - logging define in base function
    return: list of all domains in this category'''
    bs = makebs(url.format(1000000))
    bs = bs.find('div', {'class':'page_nav'})
    if bs is None:
        n = 1
        bs = 1
    else:
        bs = bs.find('li', {'class':'last'})
    if bs is None:
        log.error('On url: {} - ERROR at {}!!!'.format(url, dt.datetime.now()))
        return []
    elif bs == 1:
        pass
    else:
        n = int(bs.get_text())
    log.info('Number of page on category {} is {}'.format(url.split('/')[7], n))
    cat_list = []
    for i in range(0, n):
        cat_list.extend(one_page_parse(url.format(i), log))
    return cat_list

def one_page_parse(url, log):
    '''This function parse all domains from one page
    Parameters: url - url of page for parse domain
                log - logging define in base function
    return: list of all domains on this page'''
    bs = makebs(url)
    bs = bs.find('table', {'class': 'tableContent'}).find_all('td', {'class':'siteName1'})
    if bs is None:
        log.error('Not table on list {} at {}!!!'.format(url, dt.datetime.now()))
        return []
    page_list = []
    for td in bs:
        link = td.find('a')
        if link is None: continue
        if 'href' in link.attrs:
            page_list.append(re.sub('www.', '', urlparse(link['href']).netloc))
    return page_list

def parse_all(df, mydir):
    '''This function realise concurent parsing of pages
    Parameters: df - data frame object with urls for parse and labels
    result: write files with domain and lab to dir'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    future_to_url = {executor.submit(temporary_write, row[-1], row[-2], mydir): row for row in df.values}
                    for future in concurrent.futures.as_completed(future_to_url):
                        future_to_url[future]
    
