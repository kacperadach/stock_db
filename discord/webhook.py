from datetime import datetime
import json

import requests

from logger import Logger

PROD_ERROR = 'https://discordapp.com/api/webhooks/329754263024697354/yO-H4bhqhhzEsP0sUYH7UytGPOX0jmu73MHm8JM7vpKBs8CKiDCu5q5mKCXRnUhUSHNm'
TEST = 'https://discordapp.com/api/webhooks/330190439821082636/YDQNzKU5YahDLETWUqjBDKjVJQaTUJpRp-c1nV61uZrqRMsUUkgvKR8CaxtKHzXN08du'
BIO_PHARM_CATALYST = 'https://discordapp.com/api/webhooks/330197086622449666/JRicV2KReEZoBZ_7nXUCCrCguQVouuRMgEGgzz4aPUM4olmZec1h50fQCAlSVP4Coqgj'

FDA_CATALYST_WEBSITE = 'https://www.biopharmcatalyst.com/calendars/fda-calendar'
HISTORICAL_CATALYST_WEBSITE = 'https://www.biopharmcatalyst.com/calendars/historical-catalyst-calendar'

class DiscordWebhook():

    def __init__(self):
        pass

    def alert_start(self):
        msg = 'Production Service Started'
        self.send_message(msg)

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

    def alert_BioPharmCatalyst_fda(self, event):
        body = {
            "embeds": [{
                "title": "New BioPharmCatalyst found in FDA calendar",
                "description": u"**${}**\n\n*{}*\n{}\n\n{}\n\n__{}__\n{}".format(event['symbol'], event['drug'].strip(), event['drug_description'], event['stage'], event['date'], event['event_description']),
                "url": FDA_CATALYST_WEBSITE,
                "thumbnail": {
                    "url": "http://s3-ap-southeast-2.amazonaws.com/biopharmcatalyst/prod/_facebook/logo-stacked.png"
                }
            }]
        }
        try:
            requests.post(BIO_PHARM_CATALYST, json.dumps(body), headers={"Content-Type": "application/json"})
        except:
            Logger.log('Discord Message Error')
        else:
            Logger.log('Discord Message Sent')


    def alert_BioPharmCatalyst_catalyst(self, event):
        body = {
            "embeds": [{
                "title": "New BioPharmCatalyst found in FDA calendar",
                "description": u"**${}**\n\n*{}*\n{}\n\n{}\n\n__{}__\n{}".format(event['symbol'], event['drug'].strip(), event['drug_description'], event['stage'], event['date'], event['event_description']),
                "url": HISTORICAL_CATALYST_WEBSITE,
                "thumbnail": {
                    "url": "http://s3-ap-southeast-2.amazonaws.com/biopharmcatalyst/prod/_facebook/logo-stacked.png"
                }
            }]
        }
        try:
            requests.post(BIO_PHARM_CATALYST, json.dumps(body), headers={"Content-Type": "application/json"})
        except:
            Logger.log('Discord Message Error')
        else:
            Logger.log('Discord Message Sent')