from crawlers.help_functions import *

def scrap_cat(url, cat, lab, mydir):
    '''This function write domain from all pages in url
    Parameters: url - url for parsing domains
                lab - labels for maping domain
		cat - category domains from site
                mydir - dir for save files with domain
    result: writes file with domain and lab to dir'''
    
    log = logging.getLogger('scrapError_euro')
    
    i = 1
    url_list = []
    log.info("Small cat: {} - start at {}!".format(cat, dt.datetime.now()))
    while True:
        page = makebs(url.format(i))
        if page is None:
            log.info("{} end on {} page - LAST PAGE  - at {}!" \
                     .format(cat, i-1, dt.datetime.now()))
            break
        log.info("Create page on url {}? - {}".format(url.format(i), page is None))
        page = page.find('div', {'id': 'contentEcard'})
        if page is None:
            log.info("{} end on {} page - LAST PAGE  - at {}!" \
                     .format(cat, i-1, dt.datetime.now()))
            break
        for p in page.findAll('li', {'class':'list-article vcard'}):
            log.info("Find list on url {}".format(url.format(i)))
            p = p.find('div', {'class':re.compile('main-title')}).find('a')
            if p is None: continue
            if 'href' in p.attrs:
                p = makebs(p['href'])
            else:
                continue
            if p is None: continue
            p = p.find('a', {'class':'button compUrl'})
            if p is None: continue
            url_list.append([p['href'], lab])
        i+=1

    new_df = pd.DataFrame(url_list, columns=['domain', 'lab'])
    log.info("Small cat: {} - make df with shape {} at {}!".format(cat, new_df.shape, dt.datetime.now()))
    new_df.domain = new_df.domain.apply(normDom)
    log.info("Small cat: {} - norm df with shape {} at {}!".format(cat, new_df.shape, dt.datetime.now()))
    new_df.to_csv('{}/europages_cat{}_{}_{}.csv'.format(mydir, cat, lab, len(url_list)), sep='\t', index=False, header=None)
    log.info("Small cat: {} - ends at {}!".format(cat, dt.datetime.now()))

def parse_all(df, mydir):
    '''This function realise concurent parsing of pages
    Parameters: df - data frame object with urls for parse and labels
    result: write files with domain and lab to dir'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                future_to_url = {executor.submit(scrap_cat, row[1], row[0], row[2], mydir): row for row in df.values}
                for future in concurrent.futures.as_completed(future_to_url):
                    future_to_url[future]

