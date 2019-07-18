from pymongo import MongoClient
import logging
import configparser
import json
import subprocess
import os
import time

# Initial balances:
# volentixtrez 454990000.0000 VTX
# volentixprvt 65000000.0000 VTX
# volentixprir 130000000.0000 VTX
# vtxsmsupport 99000000.0000 VTX
# vtxmesupport 99000000.0000 VTX
# vtxcontribut 156000000.0000 VTX
# volentixtrus 0.0000 VTX

def runKeosd():
  stopKeosd()
  try:
    out = os.spawnl(os.P_NOWAIT, 'keosd', '--unlock-timeout', '90000')
  except Exception as e:
    print(e)
    return 'Could not run keosd: ' + str(e)
  else:
    return 'Run keosd: ' + str(out)

def stopKeosd():
  try:
    out = subprocess.run(['cleos', 'wallet', 'stop'], timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = out.stdout.decode('utf-8')
  except subprocess.TimeoutExpired as e:
    print(e)
    out = e
  except Exception as e:
    print(e)
    out = e
  finally:
    return 'Stop keosd: ' + str(out)

def get_balances():
  accounts = ['volentixprvt', 'volentixprir', 'volentixtrez', 'vtxcontribut', 'vtxsmsupport', 'vtxmesupport', 'volentixtrus']
  balances = {}
  for x in accounts:
    try:
      out = subprocess.run(['cleos', '-u', EOS_RPC, 'get', 'currency', 'balance', 'volentixgsys', x],
                              timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
      out = out.stdout.decode('utf-8')
    except subprocess.TimeoutExpired as e:
      out = 'Timeout. Can not get balance\n' + str(e)
    except Exception as e:
      out = 'Could not get balance\n' + str(e)
    else:
      balances[x] = str(out).replace('\n','')
  return balances

def showBad(document):
  # TODO fix the conditions so that it doesn't display duplicates
  if len(document['tokey']) < 50:
    print(document['trx_id'], " - ", document['tokey'])
  if len(document['tokey']) != 53:
    print(document['trx_id'], " - ", document['tokey'])
  if float(document['amount']) <= 0:
    print(document['trx_id'], " - ", document['amount'])
  if float(document['amount']) > 5000000:
    print(document['trx_id'])
  if "EOS" not in document['tokey']:
    print(document['trx_id'], " - ", document['tokey'])
  if 'test' in document['comment']:
    print(document['trx_id'], " - ", document['comment'])
  if float(document['amount']) >= 500000 and float(document['amount']) <= 5000000:
    print(document['trx_id'], " - ", document['tokey'], " - ", document['amount'])

def execute(fromaccount, toaccount, amount, tokey, comment, nonce, trx_id):
  amount = "{0:.4f}".format(float(amount))
  body = ''
  out = ''
  try:
    body = '[\"'+fromaccount+'\",\"'+toaccount+'\",\"'+amount +' VTX'+'\",\"'+tokey+'\",\"'+'Ledger: '+comment+'\",\"'+nonce+'\"]'

    out = subprocess.run(['cleos', '-u', EOS_RPC, 'push', 'action', 'crowdfledger', 'rcrdtfr', body, '-p', 'crowdfledger'],
                          timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = out.stdout.decode('utf-8')
    out = str(out)
  except subprocess.TimeoutExpired as e:
    print('FAIL:TIMEOUT: ', fromaccount, toaccount, amount, tokey, comment, nonce)
    print('Timeout. Can not execute transaction.\n' + str(e))
    logging.error("FAIL:TIMEOUT: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
    logging.error("Timeout. Can not execute transaction.: %s" % str(e))
    return False
  except Exception as e:
    print('FAIL:EXCEPTION: ', fromaccount, toaccount, amount, tokey, comment, nonce)
    print('Could not execute transaction.\n' + str(e))
    logging.error("FAIL:EXCEPTION: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
    logging.error("Could not execute transaction: %s" % str(e))
    return False
  else:
    if 'Error' in out:
      if 'overdrawn balance' in out:
        if fromaccount == 'vtxsmsupport':
          try:
            fromaccount = "vtxmesupport"
            comment = 'Ledger: Change crowdfund pool: vtxsmsupport -> vtxmesupport'

            body = '[\"'+fromaccount+'\",\"'+toaccount+'\",\"'+amount +' VTX'+'\",\"'+tokey+'\",\"'+comment+'\",\"'+nonce+'\"]'

            out = subprocess.run(['cleos', '-u', EOS_RPC, 'push', 'action', 'crowdfledger', 'rcrdtfr', body, '-p', 'crowdfledger'],
                                  timeout=TIMEOUT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out = out.stdout.decode('utf-8')
            out = str(out)
          except subprocess.TimeoutExpired as e:
            print('FAIL:TIMEOUT:CHANGE_POOL: ', fromaccount, toaccount, amount, tokey, comment, nonce)
            print('Timeout. Can not execute transaction.\n' + str(e))
            logging.error("FAIL:TIMEOUT:CHANGE_POOL: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
            logging.error("Timeout. Can not execute transaction: %s" % str(e))
            return False
          except Exception as e:
            print('FAIL:EXCEPTION:CHANGE_POOL: ', fromaccount, toaccount, amount, tokey, comment, nonce)
            print('Could not execute transaction.\n' + str(e))
            logging.error("FAIL:EXCEPTION:CHANGE_POOL: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
            logging.error("Could not execute transaction: %s" % str(e))
            return False
          else:
            print('PASS:CHANGE_POOL: ', fromaccount, toaccount, amount, tokey, comment, nonce)
            print(out)
            logging.info("PASS:CHANGE_POOL: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
            logging.info("%s" % out)
            return True

      print('FAIL:ERROR: ', fromaccount, toaccount, amount, tokey, comment, nonce)
      print(out)
      logging.error("FAIL:ERROR: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
      logging.error("%s" % out)
      return False
    else:
      print('PASS: ', fromaccount, toaccount, amount, tokey, comment, nonce)
      print(out)
      logging.info("PASS: %s, %s, %s, %s, %s, %s, %s" % (fromaccount, toaccount, amount, tokey, comment, nonce, trx_id))
      logging.info("%s" % out)
      return True

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

  print("Unique keys: ", len(collection.distinct('tokey')))
  print("#"*45)
  balances = get_balances()
  for key, value in balances.items():
    print("%s - %s" % (key, value))
    logging.info("Balance: %s - %s" % (key, value))
  print("#"*45)

  PASSED_DOCUMENTS = 0
  logging.info("Start executing.")
  for document in collection.find():
    showBad(document)
    # stat = execute(document['fromaccount'],
    #         document['toaccount'],
    #         document['amount'],
    #         document['tokey'],
    #         document['comment'],
    #         document['nonce'],
    #         document['trx_id'])
    # if stat:
    #   PASSED_DOCUMENTS +=1
    # time.sleep(1.5)
    # print(PASSED_DOCUMENTS,"/",TOTAL_DOCUMENTS)

  print("#"*45)
  balances = get_balances()
  for key, value in balances.items():
    print("%s - %s" % (key, value))
    logging.info("Balance: %s - %s" % (key, value))
  print("#"*45)
  print("Total documents in collection:", TOTAL_DOCUMENTS)
  print("Passed documents in collection:", PASSED_DOCUMENTS)
  logging.info("Total documents in collection: %s" % TOTAL_DOCUMENTS)
  logging.info("Passed documents in collection: %s" % PASSED_DOCUMENTS)
  print("#"*45)


if __name__ == "__main__":
  filename = 'executor.log'

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
    config.read('executor_config')
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
    DB = default.get('DB') or ''
    COLLECTION = default.get('COLLECTION') or ''
    TIMEOUT = int(default.get('TIMEOUT')) or 10

  try:
    logging.info('Run executor.')
    # print(runKeosd())
    setup()
  except KeyboardInterrupt:
    # print(stopKeosd())
    logging.warning('Exit the app. Keyboard interruption.')
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    # print(stopKeosd())
    logging.error('Exit by other reason: %s', e)
    print(e)
