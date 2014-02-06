import os

HERE = os.path.abspath(os.path.dirname(__file__))
PKEY_QUERY = open(os.path.join(HERE, 'pkey.sql')).read()
FKEY_QUERY = open(os.path.join(HERE, 'fkey.sql')).read()
