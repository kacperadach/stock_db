from api.application import app

if __name__ == '__main__':
	app.run(use_reloader=False, port=5000, threaded=True)
