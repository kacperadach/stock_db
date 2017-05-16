import sys

from app.service import MainService

def run():
	if len(sys.argv) >= 2:
		MainService(sys.argv[1])
	else:
		MainService()

if __name__ == '__main__':
	run()
