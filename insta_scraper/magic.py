import argparse
from subprocess import call

parser = argparse.ArgumentParser()
parser.add_argument("-type", help="twitter or instagram, type of scraper")
parser.add_argument("-authfile", help="path to file containing the secret keys")
parser.add_argument("-username", help="Instagram username to scrape")
args = parser.parse_args()


if args.type == 'instagram':
    call(["scrapy", "crawl", "instagram", "-a", "username="+args.username, "-a", "session_file="+args.authfile])
else:
    call(["scrapy", "crawl", "twitter", "-a", "username="+args.username, "-a", "session_file="+args.authfile])