from crawlers.help_functions import *

def scrap_usa_pages(url, lab, mydir):
    '''This function write domain from all pages in url
    Parameters: url - url for parsing domains
                lab - labels for maping domain
                mydir - dir for save files with domain
    result: writes file with domain and lab to dir'''
    
    log = logging.getLogger('scrapError_usa')
    
    i = 1
    exist = True
    url_list = []
    log.info("Small cat: {} in city: {} - starts at {}!".format(url.split('/')[-1], url.split('/')[-2], dt.datetime.now()))
    while exist:
        try:
            page_url = url+'?page={}'.format(i)
            bs_cat = makebs(page_url)
            if bs_cat is None:
                log.info("{} end of {} page!".format(name_cat, i-1))
                break
        except:
            log.info("{} end of {} page!".format(name_cat, i-1))
            break
        if len(bs_cat.findAll('div', {'class':re.compile('search-results organic')})) == 0:
            log.info("{} end on {} page - NOT RESULT - at {}!".format(name_cat, i-1, dt.datetime.now()))
            break
        pagination = bs_cat.find('div',{'class':re.compile("pagination")})
        if pagination == None:
            log.info("{} end on {} page - ONLY ONE PAGE - at {}!".format(url.split('/')[-1], i-1, dt.datetime.now()))
            exist = False
        else:  
            pag = pagination.findAll('li')
            if len(pag) == 0:
                log.info("{} end on {} page - ONLY ONE PAGE  - at {}!".format(url.split('/')[-1], i, dt.datetime.now()))
                exist = False
            else:
                if pag[-1].find('a') == None:
                    log.info("{} end on {} page - B. LAST PAGE - at {}!".format(url.split('/')[-1], i, dt.datetime.now()))
                    exist = False
        i+=1
        for a in bs_cat.findAll('a', {'class': re.compile('track-visit-website')}):
            if 'href' in a.attrs: url_list.append([a['href'], lab])

    new_df = pd.DataFrame(url_list, columns=['domain', 'lab'])
    log.info("Small cat: {} - make df with shape {} at {}!".format(url.split('/')[-1], new_df.shape, dt.datetime.now()))
    new_df.domain = new_df.domain.apply(normDom)
    log.info('DF normolize at {} with shape {}'.format(dt.datetime.now(), new_df.shape))
    new_df.to_csv('{}/usapages_{}_{}.csv'.format(mydir, url.split('/')[-2], url.split('/')[-1]), sep='\t', index=False, header=None)
    log.info("Small cat: {} in city: {} - ends at {}!".format(url.split('/')[-1], url.split('/')[-2], dt.datetime.now()))

def parse_all(df, mydir):
    '''This function realise concurent parsing of pages
    Parameters: df - data frame object with urls for parse and labels
    result: write files with domain and lab to dir'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                    future_to_url = {executor.submit(scrap_usa_pages, row[1], row[2], mydir): row for row in df.values}
                    for future in concurrent.futures.as_completed(future_to_url):
                        future_to_url[future]

