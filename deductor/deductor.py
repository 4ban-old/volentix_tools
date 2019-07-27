from pymongo import MongoClient
import logging
import configparser
import json
import subprocess
import os
import time
import random


def execute(id, key, from_account, amount, to_account):
  out = ''
  try:
    data = '["'+from_account+'", "'+to_account+'", "'+amount+' VTX", "Deduction. trx_id:'+id+'"]'
    out = subprocess.run(['cleos', '-u', EOS_RPC, 'push', 'action', 'volentixgsys', 'transfer', data, '-p', from_account], timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = out.stdout.decode('utf-8')
    out = str(out)
  except subprocess.TimeoutExpired as e:
    print('FAIL:TIMEOUT: ', id, key, from_account, amount, to_account)
    print('Timeout. Can not execute transaction.\n' + str(e))
    logging.error("FAIL:TIMEOUT: %s - %s - %s - %s - %s" % (id, key, from_account, amount, to_account))
    logging.error("Timeout. Can not execute transaction.: %s" % str(e))
    return False
  except Exception as e:
    print('FAIL:EXCEPTION: ', id, key, from_account, amount, to_account)
    print('Could not execute transaction.\n' + str(e))
    logging.error("FAIL:EXCEPTION: %s - %s - %s - %s - %s" % (id, key, from_account, amount, to_account))
    logging.error("Could not execute transaction: %s" % str(e))
    return False
  else:
    if 'Error' in out:
      print('FAIL:ERROR: ', id, key, from_account, amount, to_account)
      print(out)
      logging.error("FAIL:ERROR: %s - %s - %s - %s - %s" % (id, key, from_account, amount, to_account))
      logging.error("%s" % out)
      return False
    else:
      print('PASS: %s - %s - %s - %s - %s' % (id, key, from_account, amount, to_account))
      # print(out)
      logging.info("PASS: %s - %s - %s - %s - %s" % (id, key, from_account, amount, to_account))
      logging.info("%s" % out)
      return True


def setup():
  to_execute =dict()
  with open('execute.list', 'r') as f:
    for line in f:
      raw = line.replace('\n','').split('-')
      to_execute[raw[0].strip()] = {'key':raw[1].strip(), 'from':raw[2].strip(), 'amount':raw[3].strip(), 'to':raw[4].replace('[\'','').replace('\']','').strip()}

  for key, value in to_execute.items():
    execute(key, value['key'], value['from'], value['amount'], value['to'])
    time.sleep(1.5)



if __name__ == "__main__":
  filename = 'deductor.log'

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
    config.read('deductor_config')
    logging.info('Config reading.')
  except Exception as e:
    logging.error('Could not read the config: %s', e)
    print(e)
  else:
    default = config['DEFAULT']
    EOS_RPC = default.get('EOS_RPC') or ''
    TIMEOUT = int(default.get('TIMEOUT')) or 10

  try:
    logging.info('Run deductor.')
    setup()
  except KeyboardInterrupt:
    logging.warning('Exit the app. Keyboard interruption.')
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    logging.error('Exit by other reason: %s', e)
    print(e)
