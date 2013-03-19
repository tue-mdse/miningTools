'''
Small script to download mailing list archives (gzip mbox).
It comes in two flavours:
- downloadArchives crawls a <startURL> and finds the archives
  linked on it, potentially with one level of indirection;
- downloadArchivesList takes a list of urls listing .txt.gz 
  archives and downloads those.

@author: Bogdan Vasilescu
'''

import os
from spider import Spider            # Spider to crawl the mailing list webpage
from downloader import Downloader    # Downloader module (for the mailing list archive files)
from Queue import Queue              # Multi-threading support for Downloader
import itertools



def initDownloader(numThreads):
    '''Set up downloader'''
    queue = Queue()
    threads = []
    '''Use <numThreads> threads for download'''
    for _ in itertools.repeat(None, numThreads):
        threads.append(Downloader(queue))
        threads[-1].start()
    return queue


def stopDownloader(queue, numThreads):
    for _ in itertools.repeat(None, numThreads):
        queue.put((None, None))


def addToQ(queue, urlList, container):
    '''Download each archive'''
    for url in urlList:
        '''Strip url to extract filename'''
        urlStripped = url.strip()
        filename = urlStripped.split('/')[-1]
        
        '''Download each archive to the mailing list folder'''
        filepath = os.path.join(container, filename)
        queue.put((url, filepath))


def downloadArchives(startURL, container, lookInsideSubfolders=False, extension='.txt.gz', numThreads=5):
    '''Crawl <startURL> and find all mailing list archives (given the filename <extension>).
    Store the files in the folder with the path <container>.
    If <lookInsideSubfolders>, then go one level deeper (crawl all first-order links as well).
    '''

    '''Set up downloader'''
    queue = initDownloader(numThreads)
    
    print 'Downloading archives from', startURL
        
    if not lookInsideSubfolders:
        spider = Spider(startURL)
        spider.process_page(startURL)
    
        '''Only the links to archive files are interesting:
        mailing list archive file names end with '.txt.gz' '''
        urlList = [x for x in sorted(spider.URLs) if x.endswith(extension)]
        print '%d archives' % (len(urlList))
            
        addToQ(queue, urlList, container)
            
    else:        
        spider = Spider(startURL)
        spider.process_page(startURL)
        
        for link in sorted(spider.URLs):
            subspider = Spider(link)
            subspider.process_page(link)
            
            mlName = link.split('/')[-2]
    
            '''Only the links to archive files are interesting:
            mailing list archive file names end with '.txt.gz' '''
            urlList = [x for x in sorted(subspider.URLs) if x.endswith(extension)]
            if len(urlList):
                print '%s: %d archives' % (mlName, len(urlList))
                '''Create a folder for the mailing list'''
                store = os.path.join(container, mlName)
                if not (os.path.isdir(store)):
                    os.system("mkdir %s" % store)
                
                addToQ(queue, urlList, store)
                    
    '''If here, download finished. Stop threads'''
    stopDownloader(queue, numThreads)



def downloadArchivesList(aList, container, extension='.txt.gz', numThreads=5):
    '''Set up downloader'''
    queue = initDownloader(numThreads)

    import csv
    f = open(aList, 'rb')
    reader = csv.reader(f)
    for row in reader:
        startURL = row[0]
        
        mlName = startURL.split('/')[-2]
        
        spider = Spider(startURL)
        spider.process_page(startURL)
            
        '''Only the links to archive files are interesting:
        mailing list archive file names end with '.txt.gz' '''
        urlList = [x for x in sorted(spider.URLs) if x.endswith(extension)]
        if len(urlList):
            print '%s: %d archives' % (mlName, len(urlList))
            store = os.path.join(container, mlName)
            if not (os.path.isdir(store)):
                    os.system("mkdir %s" % store)
                
            '''Download each archive'''
            addToQ(queue, urlList, store)
                        
    '''If here, download finished. Stop threads'''
    stopDownloader(queue, numThreads)




dataPath = os.path.abspath('/Volumes/SAMSUNG/mailinglists')

'''
UBUNTU
List of archives extracted from: https://lists.ubuntu.com/mailman/admin
'''
aList = os.path.abspath('./data/ubuntu_lists.txt')
container = os.path.join(dataPath, 'ubuntu')
if not (os.path.isdir(container)):
    os.system("mkdir %s" % container)
downloadArchivesList(aList, container, extension='.txt.gz', numThreads=5)

'''
PYTHON
List of archives extracted from: http://mail.python.org/mailman/listinfo
'''
aList = os.path.abspath('./data/python_lists.txt')
container = os.path.join(dataPath, 'python')
if not (os.path.isdir(container)):
    os.system("mkdir %s" % container)
downloadArchivesList(aList, container, extension='.txt.gz', numThreads=5)

'''
R
List of archives extracted from: https://stat.ethz.ch/mailman/listinfo
'''
aList = os.path.join('./data/r_lists.txt')
container = os.path.join(dataPath, 'r')
if not (os.path.isdir(container)):
    os.system("mkdir %s" % container)
downloadArchivesList(aList, container, extension='.txt.gz', numThreads=5)


