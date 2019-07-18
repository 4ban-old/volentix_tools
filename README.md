# Volentix toolkit
A set of scripts, tools required for the migration process.

## Updater
Script will use the crowdfund API to get the correct pool for existing transaction and update the record in the MongoDB collection or delete its document if the transaction is not valid.

## Executor
Script will iterate through collection of transactions and call new ledger smart contract for executing the transactions on it with the new flow such as automatic deduction of tokens from pools to trust account.

## Cleaner

## Creator