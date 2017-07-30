import sys

from app.service import MainService

def run():
	try:
		MainService()
	except BaseException:
		from discord.webhook import DiscordWebhook
		if len(sys.argv) > 1:
			DiscordWebhook().alert_stop()

if __name__ == '__main__':
	run()
