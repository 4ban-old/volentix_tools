from pymongo import MongoClient
import logging
import configparser
import json
import subprocess
import os
import time
import random


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
  accounts_to_create = {}
  with open('final.list', 'r') as f:
    for line in f:
      (key, val) = line.replace('\n', '').split('-')
      accounts_to_create[key.strip()] = val.strip()

  for key, value in accounts_to_create.items():
    success = execute(key, value)
    if success:
      time.sleep(1.5)

if __name__ == "__main__":
  filename = 'runner.log'

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
    logging.info('Run runner.')
    setup()
  except KeyboardInterrupt:
    logging.warning('Exit the app. Keyboard interruption.')
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    logging.error('Exit by other reason: %s', e)
    print(e)
