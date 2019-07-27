from pymongo import MongoClient
import urllib.parse
import requests
import logging
import configparser
import json


def get_pool(trx_id):
  try:
    data = requests.get(POOL_API+trx_id)
  except Exception as e:
    print(e)
    logging.error("Couldn't get the data: %s" % e)
  else:
    raw = json.loads(data.text)

  if raw['success']:
    return raw['pool'].strip()
  else:
    return ''

def setup():
  try:
    client = MongoClient(MONGODB_URL,username=MONGODB_USER,password=MONGODB_PASS)
  except Exception as e:
    logging.error('Connect to the mongo: %s' % e)
    print(e)

  try:
    db = client[DB]
  except Exception as e:
    logging.error('Mongo database: %s' % e)
    print(e)

  try:
    collection = db[COLLECTION]
    TOTAL_DOCUMENTS = collection.count_documents({})
  except Exception as e:
    logging.error('Mongo collection: %s' % e)
    print(e)

  UPDATED_DOCUMENTS = 0

  for document in collection.find():
    pool = get_pool(document['trx_id'])
    # Update transaction if there is a pool for it
    if pool:
      try:
        collection.update_one({"trx_id": document['trx_id']}, {"$set": {"fromaccount": pool}})
      except Exception as e:
        print("Couldn't update: %s, %s" % (document['trx_id'], e))
        logging.error("Couldn't update: %s, with pool: %s. Error: %s" % (document['trx_id'], pool, e))
      else:
        logging.debug("Update: [%s] with pool: %s" % (document, pool))
        print("%s = %s -> %s" % (document['trx_id'], document['fromaccount'], pool))
        logging.info("%s = %s -> %s" % (document['trx_id'], document['fromaccount'], pool))
        UPDATED_DOCUMENTS+=1
    else:
      # Update transaction with private pool transaction is valid and there is no pool for it
      if 'EOS' in document['tokey'] and len(document['tokey']) > 50:
        try:
          collection.update_one({"trx_id": document['trx_id']}, {"$set": {"fromaccount": 'volentixprvt'}})
        except Exception as e:
          print("Couldn't update: %s, %s" % (document['trx_id'], e))
          logging.error("Couldn't update: %s, with pool: %s. Error: %s" % (document['trx_id'], 'volentixprvt', e))
        else:
          logging.debug("Update: [%s] with pool: %s" % (document, 'volentixprvt'))
          print("%s = %s --> %s" % (document['trx_id'], document['fromaccount'], 'volentixprvt'))
          logging.info("%s = %s -> %s" % (document['trx_id'], document['fromaccount'], 'volentixprvt'))
          UPDATED_DOCUMENTS+=1
      # Delete transaction if it is not valid
      # else:
      #   try:
      #     collection.delete_one({"trx_id": document['trx_id']})
      #   except Exception as e:
      #     print("Couldn't delete: %s, %s" % (document['trx_id'], e))
      #     logging.error("Couldn't delete: %s. Error: %s" % (document['trx_id'], e))
      #   else:
      #     logging.debug("Delete: [%s]" % (document))
      #     print("%s = %s -> [IGNORE]" % (document['trx_id'], document['fromaccount']))
      #     logging.info("%s = %s -> [IGNORE]" % (document['trx_id'], document['fromaccount']))

  print("#"*45)
  print("Total documents in collection:", TOTAL_DOCUMENTS)
  print("Updated documents in collection:", UPDATED_DOCUMENTS)
  logging.info("Total documents in collection: %s" % TOTAL_DOCUMENTS)
  logging.info("Updated documents in collection: %s" % UPDATED_DOCUMENTS)
  print("#"*45)


def upd():
  try:
    client = MongoClient(
        MONGODB_URL, username=MONGODB_USER, password=MONGODB_PASS)
  except Exception as e:
    logging.error('Connect to the mongo: %s' % e)
    print(e)

  try:
    db = client[DB]
  except Exception as e:
    logging.error('Mongo database: %s' % e)
    print(e)

  try:
    collection = db[COLLECTION]
    TOTAL_DOCUMENTS = collection.count_documents({})
  except Exception as e:
    logging.error('Mongo collection: %s' % e)
    print(e)

  try:
    collection.update_many({"toaccount": "vtxtrust"}, {"$set": {"toaccount": "volentixtrus"}})
  except Exception as e:
    print("Couldn't update: %s" % e)

  UPDATED_DOCUMENTS = collection.count_documents({'toaccount': 'volentixtrus'})
  print("#"*45)
  print("Total documents in collection:", TOTAL_DOCUMENTS)
  print("Updated documents in collection:", UPDATED_DOCUMENTS)
  logging.info("Total documents in collection: %s" % TOTAL_DOCUMENTS)
  logging.info("Updated documents in collection: %s" % UPDATED_DOCUMENTS)
  print("#"*45)

if __name__ == "__main__":
  filename = 'updater.log'

  try:
    open(filename, 'w').close()
  except Exception as e:
    print(e)

  try:
    logging.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s',
                        filename=filename,
                        level=logging.DEBUG,
                        datefmt='%m/%d/%Y %I:%M:%S %p')
  except Exception as e:
    logging.error('ConfigParser: %s' % e)
    print(e)
  else:
    logging.info('Launch the app.')

  config = configparser.ConfigParser()

  try:
    config.read('updater_config')
    logging.info('Config reading.')
  except Exception as e:
    logging.error('Could not read the config: %s', e)
    print(e)
  else:
    default = config['DEFAULT']
    MONGODB_URL = default.get('MONGODB_URL') or ''
    MONGODB_USER = default.get('MONGODB_USER') or ''
    MONGODB_PASS = default.get('MONGODB_PASS') or ''
    POOL_API = default.get('POOL_API') or ''
    DB = default.get('DB') or ''
    COLLECTION = default.get('COLLECTION') or ''

  try:
    logging.info('Run updater.')
    setup()
    # upd()
  except KeyboardInterrupt:
    logging.warning('Exit the app. Keyboard interruption.')
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    logging.error('Exit by other reason: %s', e)
    print(e)
