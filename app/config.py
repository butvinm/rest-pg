"""Application config."""

from os import environ

from dotenv import load_dotenv

if environ.get('DEBUG'):
    load_dotenv()

# It is fine to fall at the application start, if some env wasn't provided

PG_HOST = environ['POSTGRES_HOST']

PG_PORT = int(environ['POSTGRES_PORT'])

PG_USER = environ['POSTGRES_USER']

PG_PASSWORD = environ['POSTGRES_PASSWORD']

PG_DATABASE = environ['POSTGRES_DB']
