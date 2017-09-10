COMMAND_LINE_ARGUMENTS = {
	'env': 'dev',
	'use_tor': False
}

class Config():

    def __init__(self):
        for key, value in COMMAND_LINE_ARGUMENTS.iteritems():
            setattr(self, key, value)

    def set_config(self, configuration):
        if len(configuration) <= 1:
            pass
        else:
            set_keys = []
            for i in range(1, len(configuration)):
                key, value = configuration[i].split('=')
                if key.lower() in COMMAND_LINE_ARGUMENTS.iterkeys():
                    setattr(self, key, value)
                    set_keys.append(key)
        for key, value in COMMAND_LINE_ARGUMENTS.iteritems():
            if not hasattr(self, key):
                if isinstance(value, bool):
                    setattr(self, key, bool(value))
                else:
                    setattr(self, key, value)

App_Config = Config()

if __name__ == "__main__":
    pass