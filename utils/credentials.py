from os import path

CWD = path.dirname(path.realpath(__file__))
CREDENTIALS_FILE = 'db_cred.txt'
CREDENTIALS_PATH = path.join(path.dirname(CWD), CREDENTIALS_FILE)

class Credentials():

    def __init__(self):
        self.creds = {}
        f = open(CREDENTIALS_PATH, 'r')
        lines = f.readlines()
        for line in lines:
            try:
                key, value = line.split(':')
                self.creds[key] = value.strip('\n')
            except:
                print 'Credentials file has an error'

    def get_user(self):
        if 'user' in self.creds.keys():
            return self.creds['user']

    def get_password(self):
        if 'password' in self.creds.keys():
            return self.creds['password']

if __name__ == "__main__":
    c = Credentials()
