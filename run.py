from app.service import MainService

def run():
	try:
		MainService()
	except BaseException:
		from discord.webhook import DiscordWebhook
		DiscordWebhook().alert_stop()

if __name__ == '__main__':
	run()
