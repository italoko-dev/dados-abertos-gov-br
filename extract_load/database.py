from supabase import create_client
from supabase.lib.client_options import ClientOptions

def create_connection(credentials):
  url = credentials.get('url')
  key = credentials.get('key')
  schema = credentials.get('schema')
  if not schema:
    schema = 'public'

  opts = ClientOptions().replace(schema=schema)
  return create_client(url, key, options=opts)

def fetch_data(client, table, columns):
  response = client.table(table).select(*columns).execute()
  return response.data

def append_data(client, table, data):
  data = data.to_dict('records')
  data, count = client.table(table).insert(data).execute()
  if data:
    return True
  return False