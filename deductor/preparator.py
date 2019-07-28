from pymongo import MongoClient
import logging
import configparser
import requests
import json
import subprocess
import os
import time
import random
import string

# volentixtrez 454990000.0000 VTX
# volentixprvt 65000000.0000 VTX
# volentixprir 130000000.0000 VTX
# vtxsmsupport 99000000.0000 VTX
# vtxmesupport 99000000.0000 VTX
# vtxcontribut 156000000.0000 VTX
# volentixtrus 0.0000 VTX

IGNORE = ['4d8c0d7f1fdbbc3431af5d42b41156fe3e5c71610e8156ced118526585fae93e',
          '074cae82b2d1b378471a91acb747685d67c328b98f2b146e602a557376894985',
          'c0eedbf7f9dfe7667215858b5cc9409e0f3392f091db5ae188b8e7d9bc35f58b',
          '8dbf9366c9b07d73ed66b4befc771904edc348669e12bf4e75c4665f1bc7587b',
          '9d654e9f88d877f14872414e1dce1bb7f1418277497d3469f31c6ce4a96b6656',
          '0c097f6583fc4bd5ba13f69b4835049fb7454df7997d8a14fa42e339e7efe3be',
          '5713b36e24f9c0f8e05629f954b2a1d25818c86aed597a4b2a0e0857825acfec',
          'aa76534cad4fd8e05b4eddd9791b3bfad421f6eef266bc835dab1bfd088a88a8',
          '26442fbfd6f1ddd50e1ca64add6b553359122913f60974ad6298df3b2e3b8141',
          'af7ba352ddf2af60ba76627a0f861281deaf62e3dcd3b6b8a541d016f30b5c4c',
          '8c3cebace66ab62f71b58d4a3b67487c4dd29f4fa5522e9d6e60182cc0c5ca52',
          '9cbb15b98ff3e6c0290784fe14a0d5f98707e990d0c51813c72d643b8bcbdf82',
          '448c2308ceec2710a2cf35695718563fa3243d813a6de3669672331a0e4040b4',
          'd7096a1e0ef05cf3db73334961283cd038ef30a4094561a92041eaecf9821049',
          '8d2be77b12c6084709f0d11aa994fbdb64624ed4102305527155c461cb2826a4',
          ]

NOT_SURE = ['835bcfd2a47b09639427c1ac6539b73b716fdb2836fa1074bc29e709b78a12d5',
            '29e5c450ed1b2a22f92670e153cfd35371f62088a04fd3b99e3552ee0c226119',
            '2616ee61582d1b2bbdeb9d4be6c90105a720727883722b7b320c7f4a65c40bb5',
            'f0fb35299cd87e5e2567e26d625e7f2320b942bc0ae5254af0842afa8719b5a0',
            '2af855428bf3af6238adae13efdaf289b67b754b3aff7fc04f28723b4f11dd8b',
            '96fbdb3cc900afbacfe43debbaadfac6430c4dfcef7993afc6ccd7e42abe28b3',
            '286c07710de1d5007ffeaf582afe59fa2bd769e80a7ceeb581f5ca22aa2ed312',
            '664a97eba801039730f609ef13918c200afd5bb132de27c4a77064c1fb6036b1',
            '5b1c371264c3ea00480ef0e60a3d69d433aabef915de3a9d7fe6dc66aa3282e0',
            'f4178f2fc31dd30d75df1b03767cba24fa8f3ac738725d8f0d4f46a703ac2b11',
            '90bd1ff92bc588648fe00093537c3596415db8efe18ea4f69ae55e9eed1203bc',
            '1a886672d7c704e462c9c9459209592261b00d773af9751123b64ebb2d393b4e',
            'ada7dbafe02168d63d7c891c9f16c90cc49c20d80de1a7310a0d118cb292b474',
            '2588b94193d01a3076ea52ef88afc55c82c1d19259f044c0a354ff60e665b9df',
            '9b8b65d3bea4466bd3e0a28b309f4c96e924b224ea39c540fac4a7ec26ed3fad',
            'bf707aba8cfeb85cbe27b62238614cf2f57e618f95ddcbfcb32c592ac3b8a868',
            '420cfea73ed6b410e62f2d19fac0ca391f2758d566e4ba2ead79cfb2ed63c263',
            '1652c961f19a75727b266147f68c03c1e75c3a0538772cfec8f6907c437de8f6',
            '54c8ebdc0dc181e937707977206d4de09a3e70a914c72cf52163159747c8a89b',
            'd739ee7156c3432c219fb70bafaa4ff41a38df63598a6370c3010b457c67fd52',
            '8ecdd32b9c399176ea395cd466a27649e030716ba9818c8b6aca02fb199e75b3',
            '2115065c74a2ca19f21a577e24fe5cfdeb0015412ee24a21d2d76c5848fe76e8',
            'b3d881bdfb0f4ad694404aa68bd0be764d1f63e9ce5d4a52ac39c1d9dcff50c5',
            '5aa4d4d6a84832ea28245a6d8edf632bf55278e0b9057f2aef8511ecc9e64fb3',
            '444fc2362e32f49849ccb7768c1222fd399d445674552b1304b1769e3dadff41',
            '2e6023872166063ad06b0f6cfe616935f884c109e10b14d30597ea3609f2c7d9',
            'ef6aac24992fe3ed827f3bda2234b02d6334852e17b128dfdd5258e020706c03',
            'c18b30ee0f576dd8c861fb56f6bfdcbf10449c25979f68a00bacafa636db98ec',
            'd093efd38d8571ccd7ecb10f997e2ed265ba06eef2f81d5b564df976eb8dfc66',
            '788247cf3c798512c8456c05a202aa9d2ec520dad9e6d69769373cb60b14caaf',
            'f409c2bdffd621f5296a0157f9e7f397cbfdc0be063ec9106fc2f67df649c77c',
            '0d08a3bcbb6fdc995661b7a1f4b17d3e92fce9e9e59113dc1809b5a4db4dd356',
            'f244c36767cd87cba09766e4dfd8fda40df290ecfe3f48da900ce9e8723bfee3',
            'c9971a942fb9533dab7ad2108bc7cdaacaf62c300547154aff07d8f74722bac6',
            ]

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

def get_stats(collection):
   # Total tokens will be sent
  total_tokens = 0
  vtxsmsupport = 0
  volentixprvt = 0
  volentixprir = 0
  volentixtrez = 0
  vtxcontribut = 0

  for document in collection.find():
    if document['trx_id'] in IGNORE or document['trx_id'] in NOT_SURE:
      continue
    else:
      if float(document['amount']) <= 1000:
        total_tokens += float(document['amount'])
        if document['fromaccount'] == 'vtxsmsupport':
          vtxsmsupport += float(document['amount'])
        if document['fromaccount'] == 'volentixprvt':
          volentixprvt += float(document['amount'])
        if document['fromaccount'] == 'volentixprir':
          volentixprir += float(document['amount'])
        if document['fromaccount'] == 'volentixtrez':
          volentixtrez += float(document['amount'])
        if document['fromaccount'] == 'vtxcontribut':
          vtxcontribut += float(document['amount'])

  print('total: ', total_tokens)
  print('vtxsmsupport: ', vtxsmsupport)
  print('volentixprvt: ', volentixprvt)
  print('volentixprir: ', volentixprir)
  print('volentixtrez: ', volentixtrez)
  print('vtxcontribut: ', vtxcontribut)

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

  with open('ignore.list', 'w') as f:
    for trx in IGNORE:
      f.write("%s\n" % (trx))
    for trx in NOT_SURE:
      f.write("%s\n" % (trx))
  print(EOS_RPC)
  print('TOTAL: ', TOTAL_DOCUMENTS)
  # total number of tokens to send
  # get_stats(collection)

  counter = 1
  with open('execute.list', 'w') as f:
    for document in collection.find():
      # Remove blacklisted transactions
      if document['trx_id'] in IGNORE or document['trx_id'] in NOT_SURE:
        continue
      else:
        # Remove transactions below 1000
        if float(document['amount']) <= 1000:
          amount = "{0:.4f}".format(float(document['amount']))
          account = get_account(document['tokey'].strip())['account_names']
          print(counter, document['trx_id'], document['tokey'], document['fromaccount'], amount, account)
          logging.info("%s - %s - %s - %s - %s" % (document['trx_id'], document['tokey'], document['fromaccount'], amount, account))
          f.write("%s - %s - %s - %s - %s\n" % (document['trx_id'], document['tokey'], document['fromaccount'], amount, account))
          amount = ''
          account = ''
          counter +=1


  print("*"*150)

  logging.info("Start executing.")

if __name__ == "__main__":
  filename = 'preparator.log'

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
    MONGODB_URL = default.get('MONGODB_URL') or ''
    MONGODB_USER = default.get('MONGODB_USER') or ''
    MONGODB_PASS = default.get('MONGODB_PASS') or ''
    EOS_RPC = default.get('EOS_RPC') or ''
    POOL_API = default.get('POOL_API') or ''
    EOS_CREATOR = default.get('EOS_CREATOR') or ''
    DB = default.get('DB') or ''
    COLLECTION = default.get('COLLECTION') or ''
    TIMEOUT = int(default.get('TIMEOUT')) or 10

  try:
    logging.info('Run preparator.')
    setup()
  except KeyboardInterrupt:
    logging.warning('Exit the app. Keyboard interruption.')
    print('\nKeyboard interruption. \nExit.')
  except Exception as e:
    logging.error('Exit by other reason: %s', e)
    print(e)
