from crawlers.help_functions import *
import imp


def from_start_to_end(mydir, file, file_to_save,parsing_function):
    '''This function make dir for write files with domain and labels and than aggregate
    it in one file and remove auxiliary files and dir
    Parameters: mydir - name of dir for write auxiliary files
                file - file with urls for parse and labels
		file_to_save - file name for save aggregate data frame
		parsing_function - function for parsing domain by url
    result: write aggregate file with domains and labels'''
    os.makedirs(mydir, exist_ok=True)
    df = pd.read_csv(file, sep='\t')
    parsing_function(df, mydir)
    df = pd.DataFrame()
    for file in os.listdir(mydir):
        if file.endswith('.csv'):
            d = pd.read_csv(os.path.join(mydir, file), sep='\t', names=['domain', 'lab'])
            df = pd.concat([df, d], axis=0)
    try:
        df['lab'] = df['lab'].astype(int)
    except:
        pass
    df['lab'] = df['lab'].astype(str)
    df = df.drop_duplicates().sort_values('lab')
    df = df.groupby('domain').agg({'lab':lambda r: ','.join(r)})
    df.lab = df.lab.apply(lambda r: ','.join(r.split(',')[:3]))
    df.to_csv(file_to_save, sep='\t', header=False)
    for file in os.listdir(mydir):
        if file.endswith('.csv'):
            os.remove(os.path.join(mydir, file))
    os.rmdir(mydir)

def import_crawlers():
    '''This function return all crawlers from dir 'crawlers'
    return: dict with crawlers'''
    crawler_path = 'crawlers/'
    crawlers = {}
    for crawler in os.listdir(crawler_path):
        if crawler.endswith('parser.py'):
            crawler = crawler.split('.')[0]
            module = imp.find_module(os.path.join(crawler_path, crawler))
            crawlers.update({crawler.split('_')[0]:imp.load_module(crawler, *module)})    
    return crawlers
        
# Start
def main():
    
    crawlers = import_crawlers()
    path = 'url_lists_for_map/'
    
    for pattern, parser in zip(crawlers.keys(), crawlers.values()):
        for csv_file in os.listdir(path):
            if csv_file.startswith(pattern):
                url_list = os.path.join(path, csv_file)

        mydir = '{}_pages'.format(pattern)
        target_file = '{}_to_base.csv'.format(pattern)

        from_start_to_end(mydir, url_list, target_file, parser.parse_all)
  
    df = pd.DataFrame()
    for file in os.listdir():
        if file.endswith('.csv'):
            d = pd.read_csv(file, sep='\t', names=['domain', 'lab'])
            df = pd.concat([df, d], axis=0)
    df.to_csv('yellow_pages_to_bd.csv', sep='/t', index=False, header=False)
    put_path = "https://forest1.getaura.ru/feeds/denis-feeds/yellow_pages_to_bd.csv"
    resp = requests.put(put_path, data=open('yellow_pages_to_bd.csv', 'rb'))
    for file in os.listdir():
        if file.endswith('.csv'):
            os.remove(file)
    
if __name__ == '__main__':
    exit(main())
