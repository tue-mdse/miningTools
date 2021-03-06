'''
Solution by McJeff to a question on StackOverflow:
"How would I download files with Python using wget and save them locally?"
http://stackoverflow.com/a/2467646/1285620
'''

import sys
import threading
import urllib
from Queue import Queue
import logging

class Downloader(threading.Thread):
	def __init__(self, queue):
		super(Downloader, self).__init__()
		self.queue = queue

	def run(self):
		while True:
			download_url, save_as = self.queue.get()
			# sentinal
			if not download_url:
				return
			try:
				urllib.urlretrieve(download_url, filename=save_as)
			except Exception, e:
				logging.warn("error downloading %s: %s" % (download_url, e))

if __name__ == '__main__':
	queue = Queue()
	threads = []
	for i in xrange(5):
		threads.append(Downloader(queue))
		threads[-1].start()

	for line in sys.stdin:
		url = line.strip()
		filename = url.split('/')[-1]
		print "Download %s as %s" % (url, filename)
		queue.put((url, filename))
		
	# if we get here, stdin has gotten the ^D
	print "Finishing current downloads"
	for i in xrange(5):
		queue.put((None, None))