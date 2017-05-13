import sys

from app.service import main_service

if __name__ == '__main__':
	if len(sys.argv) >= 2:
		main_service(sys.argv[1])
	else:
		main_service()