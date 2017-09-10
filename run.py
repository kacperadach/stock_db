#!/usr/bin/python
import sys

from app.service import MainService

def run():
	try:
		MainService()
	except BaseException as e:
		from discord.webhook import DiscordWebhook
		if len(sys.argv) > 1 and sys.argv[1].lower() == 'prod':
			DiscordWebhook().alert_stop()

if __name__ == '__main__':
	run()
