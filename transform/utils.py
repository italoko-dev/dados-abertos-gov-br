from configparser import ConfigParser

def get_config(section, settings_path='.', filename='settings.ini'):

    settings_file = f'{settings_path}/{filename}'

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(settings_file)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, settings_file))

    return config