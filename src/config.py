# Define configuration default values
DEFAULTS = {
    'es.server': 'elasticsearch',
    'es.timeout': 60,
    'testdata.path': './test-data',
    'testdata.maxfiles': -1, # -1 for unlimited
    'testdata.create.event': 1,
}

# get a configuration value for the specified key from
# the environment, falling back to default value if undefined.
def get(key):
    import os
    if key in DEFAULTS:
        cast_type = type(DEFAULTS[key])
        return cast_type(os.environ.get(key, DEFAULTS[key]))
    else:
        return os.environ[key]

