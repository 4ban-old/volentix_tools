from pymongo import MongoClient
import logging
import configparser
import json
import subprocess
import os
import time
import random
import string

# The names that are in the list by default are blacklisted and will not be used for assigning.
USED_NAMES = ['volentixanus', 'volentixarse', 'volentixbutt', 'volentixclit',
              'volentixcock', 'volentixcoon', 'volentixcunt', 'volentixdago',
              'volentixdamn', 'volentixdick', 'volentixdike', 'volentixdyke',
              'volentixfuck', 'volentixgook', 'volentixheeb', 'volentixheck',
              'volentixhell', 'volentixhomo', 'volentixjizz', 'volentixkike',
              'volentixkunt', 'volentixkyke', 'volentixmick', 'volentixmuff',
              'volentixpaki', 'volentixpiss', 'volentixpoon', 'volentixputo',
              'volentixshit', 'volentixshiz', 'volentixslut', 'volentixsmeg',
              'volentixspic', 'volentixtard', 'volentixtits', 'volentixtwat',
              'volentixwank', 'volentixcrap', 'volentixniga', 'volentixnigg']
PREFIX = 'volentix'

def get_account(key):
  try:
    out = subprocess.run(['cleos', '-u', EOS_RPC, 'get', 'accounts', key],
                          timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = out.stdout.decode('utf-8')
  except subprocess.TimeoutExpired as e:
    out = 'Timeout. Can not get accounts\n' + str(e)
  except Exception as e:
    out = 'Could not get accounts\n' + str(e)
  else:
    if 'Error' in out:
      return out
    else:
      return json.loads(out)

def get_unique_keys(collection):
  removed = []
  keys = []
  rawkeys = collection.distinct('tokey')

  for key in rawkeys:
    if 'EOS' not in key or len(key) != 53:
      removed.append(key)
    else:
      keys.append(key)
  return keys, removed

def name_validation(name):
  try:
    out = subprocess.run(['cleos', '-u', EOS_RPC, 'get', 'account', name],
                        timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = out.stdout.decode('utf-8')
  except subprocess.TimeoutExpired as e:
    out = 'fail: Timeout. Can not get account\n' + str(e)
  except Exception as e:
    out = 'fail: Could not get account\n' + str(e)
  else:
    if 'fail' in out:
      return False
    if 'error' in out:
      return True
    elif 'created' in out:
      return False

def get_new_name():
  while True:
    name = PREFIX
    for x in range(4):
      # n = random.randint(1, 5)
      n = random.choice(string.ascii_lowercase)
      name += n
    if name_validation(name):
      if name in USED_NAMES:
        continue
      else:
        USED_NAMES.append(name)
        return name
    else:
      continue

def execute(key, name):
  out = ''
  try:
    out = subprocess.run(['cleos', '-u', EOS_RPC, 'system', 'newaccount', EOS_CREATOR, name, key, key, '--stake-net', '0.001 EOS', '--stake-cpu', '0.001 EOS', '--buy-ram-kbytes', '3', '-p' , EOS_CREATOR], timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = out.stdout.decode('utf-8')
    out = str(out)
  except subprocess.TimeoutExpired as e:
    print('FAIL:TIMEOUT: ', key, name)
    print('Timeout. Can not execute transaction.\n' + str(e))
    logging.error("FAIL:TIMEOUT: %s - %s" % (key, name))
    logging.error("Timeout. Can not execute transaction.: %s" % str(e))
    return False
  except Exception as e:
    print('FAIL:EXCEPTION: ', key, name)
    print('Could not execute transaction.\n' + str(e))
    logging.error("FAIL:EXCEPTION: %s - %s" % (key, name))
    logging.error("Could not execute transaction: %s" % str(e))
    return False
  else:
    if 'Error' in out:
      print('FAIL:ERROR: ', key, name)
      print(out)
      logging.error("FAIL:ERROR: %s - %s" % (key, name))
      logging.error("%s" % out)
      return False
    else:
      print('PASS: %s - %s' % (key, name))
      # print(out)
      logging.info("PASS: %s - %s" % (key, name))
      logging.info("%s" % out)
      return True


def setup():
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

  print(EOS_RPC)
  keys, removed = get_unique_keys(collection)
  print("Unique keys: %s" % (len(keys)))
  print("Removed keys: %s" % (len(removed)))
  logging.info('Unique keys length: %s' % len(keys))
  logging.info('Removed keys length: %s' % len(removed))
  logging.info('Unique keys: %s' % keys)
  logging.info('Removed keys: %s' % removed)
  print("*"*150)

  logging.info("Start executing.")

  # Check the account existence, asign new name
  accounts_to_ignore = dict()
  accounts_to_create = dict()
  with open('cata.log', 'w') as f:
    for key in keys:
      account = get_account(key)
      if "Error" in account:
        accounts_to_ignore[key] = 'invalid_key'
        print("%s - %s - %s [ignore]" % (keys.index(key), key, accounts_to_ignore[key]))
        f.write("%s - %s - %s\n" % (keys.index(key), key, accounts_to_ignore[key]))
      else:
        if account['account_names']:
          accounts_to_ignore[key] = account['account_names'][0]
          print("%s - %s - %s [ignore]" % (keys.index(key), key, accounts_to_ignore[key]))
          f.write("%s - %s - %s\n" % (keys.index(key), key, accounts_to_ignore[key]))
        else:
          accounts_to_create[key] = get_new_name()
          print("%s - %s - %s" % (keys.index(key), key, accounts_to_create[key]))
          f.write("%s - %s - %s\n" % (keys.index(key), key, accounts_to_create[key]))
  print("*"*120)
  print('Accunts to ignore: %s' % len(accounts_to_ignore))
  print('Accunts to create: %s' % len(accounts_to_create))
  logging.info('Accunts to ignore: %s \n %s' % (len(accounts_to_ignore), accounts_to_ignore))
  logging.info('Accunts to create: %s \n %s' % (len(accounts_to_create), accounts_to_create))

  # iterate through the final list of accounts and create them with their public keys assigned
  # for key, value in final_accounts_dict.items():
  #   success = execute(key, value)
  #   if success:
  #     # print("Created: %s - %s" % (key, value))
  #     # logging.info("Created: %s - %s" % (key, value))
  #     time.sleep(1.5)
  #   else:
  #     pass
  #     # print("Failed: %s - %s" % (key, value))
  #     # logging.error("Failed: %s - %s" % (key, value))

  # or iterate through the final list of accounts and save them into a file
  with open('final.list','w') as f:
    for key, value in accounts_to_create.items():
      f.write("%s - %s\n" % (key, value))


if __name__ == "__main__":
  filename = 'creator.log'

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
    config.read('creator_config')
    logging.info('Config reading.')
  except Exception as e:
    logging.error('Could not read the config: %s', e)
    print(e)
  else:
    default = config['DEFAULT']
    MONGODB_URL = default.get('MONGODB_URL') or ''
    MONGODB_USER = default.get('MONGODB_USER') or ''
    MONGODB_PASS = default.get('MONGODB_PASS') or ''
    EOS_RPC = default.get('EOS_RPC') or ''
    EOS_CREATOR = default.get('EOS_CREATOR') or ''
    DB = default.get('DB') or ''
    COLLECTION = default.get('COLLECTION') or ''
    TIMEOUT = int(default.get('TIMEOUT')) or 10

  try:
    logging.info('Run creator.')
    setup()
  except KeyboardInterrupt:
    logging.warning('Exit the app. Keyboard interruption.')
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    logging.error('Exit by other reason: %s', e)
    print(e)
