# import json
#
# import requests
#
# FDA_CATALYST_WEBSITE = 'https://www.biopharmcatalyst.com/calendars/fda-calendar'
# HISTORICAL_CATALYST_WEBSITE = 'https://www.biopharmcatalyst.com/calendars/historical-catalyst-calendar'
#
# class DiscordWebhook():
#
#     def __init__(self):
#         self.name = 'DiscordWebhook'
#
#     def alert_start(self):
#         msg = 'Production Service Started'
#         self.send_message(msg, MAIN_SERVICE)
#
#     def alert_stop(self):
#         msg = 'Production Service Stopped'
#         self.send_message(msg, MAIN_SERVICE)
#
#     def alert_error(self, thread_name, error):
#         msg = 'Prod Service Error in {}: \n'.format(thread_name) + "```\n" + error + "\n```"
#         self.send_message(msg, PROD_ERROR)
#
#     def send_report(self, report):
#         self.send_message(report, REPORTING)
#
#     def send_message(self, msg, messenger):
#         try:
#             requests.post(messenger, {'content': msg})
#         except:
#             pass
#
#
#     def alert_BioPharmCatalyst_fda(self, event):
#         body = {
#             "embeds": [{
#                 "title": "New BioPharmCatalyst found in FDA calendar",
#                 "description": u"**${}**\n\n*{}*\n{}\n\n{}\n\n__{}__\n{}".format(event['symbol'], event['drug'].strip(), event['drug_description'], event['stage'], event['date'], event['event_description']),
#                 "url": FDA_CATALYST_WEBSITE,
#                 "thumbnail": {
#                     "url": "http://s3-ap-southeast-2.amazonaws.com/biopharmcatalyst/prod/_facebook/logo-stacked.png"
#                 }
#             }]
#         }
#         try:
#             requests.post(BIO_PHARM_CATALYST, json.dumps(body), headers={"Content-Type": "application/json"})
#         except:
#             pass
#
#
#     def alert_BioPharmCatalyst_catalyst(self, event):
#         body = {
#             "embeds": [{
#                 "title": "New BioPharmCatalyst found in Historical Catalyst calendar",
#                 "description": u"**${}**\n\n*{}*\n{}\n\n{}\n\n__{}__\n{}".format(event['symbol'], event['drug'].strip(), event['drug_description'], event['stage'], event['date'], event['event_description']),
#                 "url": HISTORICAL_CATALYST_WEBSITE,
#                 "thumbnail": {
#                     "url": "http://s3-ap-southeast-2.amazonaws.com/biopharmcatalyst/prod/_facebook/logo-stacked.png"
#                 }
#             }]
#         }
#         try:
#             requests.post(BIO_PHARM_CATALYST, json.dumps(body), headers={"Content-Type": "application/json"})
#         except:
#             pass
