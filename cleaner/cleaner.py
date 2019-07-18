from pymongo import MongoClient
import logging
import configparser
import json
import subprocess
import os
import time



def setup():
  i=0
  while True:
    try:
      body = '['+str(i)+']'
      out = subprocess.run(['cleos', '-u', 'http://api.kylin.alohaeos.com', 'push', 'action', 'crowdfledger', 'deletetfr', body, '-p', 'crowdfledger'],
                          timeout=3, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
      out = out.stdout.decode('utf-8')
      out = str(out)
    except subprocess.TimeoutExpired as e:
      print('Timeout. Can not execute.\n' + str(e))
      print("*"*145)
    except Exception as e:
      print('Could not execute.\n' + str(e))
      print("*"*145)
    else:
      if 'ID does not exist' in out:
        print("ID:", i, "\n",out)
        print("*"*145)
        # break
      else:
        print(out)
        print("*"*145)
        i+=1


if __name__ == "__main__":
  try:
    setup()
  except KeyboardInterrupt:
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    print(e)
