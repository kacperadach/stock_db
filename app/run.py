import sys

from app.process import main_process

if __name__ == '__main__':
	if len(sys.argv) >= 2:
		main_process(sys.argv[1])
	else:
		main_process()