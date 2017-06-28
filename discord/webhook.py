import requests

from logger import Logger

PROD_ERROR = 'https://discordapp.com/api/webhooks/329754263024697354/yO-H4bhqhhzEsP0sUYH7UytGPOX0jmu73MHm8JM7vpKBs8CKiDCu5q5mKCXRnUhUSHNm'

class DiscordWebhook():

    def __init__(self):
        pass

    def alert_error(self, thread_name, error):
        msg = 'Prod Service Error in {}: \n'.format(thread_name) + "```\n" + error + "\n```"
        self.send_message(msg)

    def send_message(self, msg):
        try:
            requests.post(PROD_ERROR, {'content': msg})
        except:
            Logger.log('Discord Message Error')
        else:
            Logger.log('Discord Message Sent')
