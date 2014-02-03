#!/usr/bin/python

from Reddit import Reddit
from DB import DB
from sys import exit
from time import sleep
from sys import stdout
from time import strftime, gmtime

SUBS_TEXT = '''
gonewild
'''

SUBS = [x.strip() for x in SUBS_TEXT.strip().split('\n')]
while '' in SUBS: SUBS.remove('')

db = DB()
hoseformat = '%s - http://redd.it/%s | %s @ r/%s by %s: %s'

last_post = db.get_config('last_post')

reddit_url = 'http://www.reddit.com/r/%s/new.json' % '+'.join(SUBS)
print 'firehose from %s' % reddit_url
while True:
	sleep(2)
	try:
		posts = Reddit.get(reddit_url)
	except Exception, e:
		print 'error when querying %s: %s' % (reddit_url, str(e))
		continue
	for post in posts:
		if last_post != None and post.id == last_post:
			break
		if post.selftext != None:
			# TODO self-text, skip it
			continue
		timestamp = strftime('[%Y-%d-%m/%H:%M:%S]', gmtime())
		hose = (hoseformat % (timestamp,
		(post.id),
		(post.url),
		(post.subreddit),
		(post.author),
		post.title)).encode('utf-8')
		print(hose)
		posttime = post.created
		url = post.url
		shorturl  = 'http://redd.it/%s' % post.id
		subreddit = post.subreddit
		author    = post.author
		title     = post.title
		db.insert('posts', [posttime, post.id, shorturl, url, subreddit, author, title,])

	if len(posts) > 0:
		last_post = posts[0].id
		db.set_config('last_post', last_post)
		db.commit()

