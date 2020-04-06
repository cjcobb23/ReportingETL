#!/usr/bin/python3

import websockets
import asyncio
import json
import io
import os
import subprocess
import argparse
import time


## Execute a function to completion. If websocket connection is closed,
## reestablish the connection and try again
async def wrap(address, func):
    while True:
        try:
            async with websockets.connect(address) as ws:
                return await func(ws)
        except websockets.exceptions.ConnectionClosedError as e:
            print("Websocket closed. Address = " + address)
            await asyncio.sleep(1)



## Fetch ledger header and tx blobs from tx. Send both to reporting
async def load_ledger_simple(txAddress, reportingAddress, ledgerSeq):
    ledgerHeader = None
    async def getLedgerHeader(txWs):
        nonlocal ledgerSeq
        nonlocal ledgerHeader
        if ledgerSeq is None:
            ledgerSeq = "validated"
        else:
            ledgerSeq = int(ledgerSeq)
        while True:
            print("Getting ledger header from tx")
            await txWs.send(json.dumps({"command":"ledger","ledger_index":ledgerSeq}))
            payload = json.loads(await txWs.recv())
            if 'error' in payload or not payload['result']['validated']:
                print("waiting for ledger : " + str(ledgerSeq))
                await asyncio.sleep(2)
                continue
            print("Got ledger header from tx")
            print(payload)
            ledgerHeader = payload['result']['ledger']
            ledgerSeq = int(ledgerHeader['ledger_index'])
            break

    await wrap(txAddress, getLedgerHeader)


    print("Sending ledger header to reporting")

    async def sendLedgerHeader(reportingWs):
        nonlocal ledgerHeader
        await reportingWs.send(json.dumps({"command": "ledger_accept","ledger":ledgerHeader}))
        payload = json.loads(await reportingWs.recv())
        print(payload)

    await wrap(reportingAddress, sendLedgerHeader)

    print("Getting txns from tx")
    txs = None
    async def getTxns(txWs):
        nonlocal ledgerSeq
        nonlocal txs
        await txWs.send(json.dumps({"command":"ledger","ledger_index":ledgerSeq,
            'transactions': True, 'expand': True, 'binary':True}))
        payload = json.loads(await txWs.recv())
        txs = payload['result']['ledger']['transactions']

    await wrap(txAddress, getTxns)

    async def sendTxns(reportingWs):
        nonlocal txs
        await reportingWs.send(json.dumps({"command":"ledger_accept", "load_txns":True,
            "transactions": txs}))
        payload = json.loads(await reportingWs.recv())
        print(payload)

    await wrap(reportingAddress, sendTxns)
    return int(ledgerSeq)


## Fetch data for all objects in a ledger. Send data to reporting
async def load_data_simple(txAddress, reportingAddress, ledgerSeq):
    marker = None
    done = False
    jsonArgs = {"command":"ledger_data","ledger_index":ledgerSeq, "binary": True}

    while not done:
        if marker is not None:
            jsonArgs['marker'] = marker
        payload = None

        async def getData(txWs):
            nonlocal jsonArgs
            await txWs.send(json.dumps(jsonArgs))
            return json.loads(await txWs.recv())

        payload = await wrap(txAddress, getData)
        if 'marker' in payload['result']:
            marker = payload['result']['marker']
        else:
            print("Done downloading data")
            marker = None
            done = True
        state = payload['result']['state']

        async def sendData(reportingWs):
            nonlocal state
            nonlocal marker
            print("sending data to reporting. marker = " + marker)
            await reportingWs.send(json.dumps({"command": "ledger_accept",
                "ledger_data":True,"state":state}))
            await reportingWs.recv()
        await wrap(reportingAddress, sendData)


## Send finish to reporting. Finish writes the ledger and its data to db
async def finish_simple(reportingAddress, ledgerSeq):
    async def wrapped(reportingWs):
        await reportingWs.send(json.dumps({"command": "ledger_accept",
            "finish":True, "ledger_index":ledgerSeq}))
        return json.loads(await reportingWs.recv());

    print("Finishing...")
    res = await wrap(reportingAddress, wrapped)
    print("Finished!")
    return res


## Load ledger, load data and finish
async def setup_simple(txIp,txPort, reportingIp, reportingPort, ledgerSeq):
    txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    ledgerSeq = await load_ledger_simple(txAddress, reportingAddress, ledgerSeq)
    await load_data_simple(txAddress, reportingAddress, ledgerSeq)
    res = await finish_simple(reportingAddress, ledgerSeq)
    print(res)
    if res['result']['account_hash'] == 'correct' and res['result']['tx_hash'] == 'correct':
        print("Correctly loaded ledger = " + str(ledgerSeq))
        return ledgerSeq
    else:
        print("mismatch at ledger = " + str(ledgerSeq))
        return None

    
## Load ledger, load modified objs and finish
async def update_simple(txIp, txPort, reportingIp, reportingPort, ledgerSeq):
    txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    ledgerSeq = int(ledgerSeq)
    while True:
        await load_ledger_simple(txAddress, reportingAddress, ledgerSeq)
        await load_diff_simple(txAddress, reportingAddress, ledgerSeq)
        res = await finish_simple(reportingAddress, ledgerSeq)
        if res['result']['account_hash'] == 'correct' and res['result']['tx_hash'] == 'correct':
            print("Correctly loaded ledger = " + str(ledgerSeq))
            ledgerSeq = ledgerSeq + 1
        else:
            print("mismatch at ledger = " + str(ledgerSeq))
            break


## Perform setup, then continously load ledgers as they are validated
async def do_all_simple(txIp, txPort, reportingIp, reportingPort, ledgerSeq):
    ledgerSeq = await setup_simple(txIp, txPort, reportingIp, reportingPort, ledgerSeq)
    if ledgerSeq is not None:
        await update_simple(txIp, txPort, reportingIp, reportingPort, ledgerSeq+1)
    else:
        print("Failed to load initial ledger")


## Determine which ledger objects were modified. Send those ledger objects to
## reporting
async def load_diff_simple(txAddress, reportingAddress, ledgerSeq):
    print("getting metadata")
    async def getMeta(txWs):
        nonlocal ledgerSeq
        await txWs.send(json.dumps(
            {"command":"ledger",
                "ledger_index": ledgerSeq,
                'transactions': True, 'expand': True}))
        return json.loads(await txWs.recv())

    payload = await wrap(txAddress, getMeta)

    print("Processing metadata")
    objs = set()
    for tx in payload['result']['ledger']['transactions']:
        meta = tx['metaData']
        for node in meta['AffectedNodes']:
            idx = None
            if 'ModifiedNode' in node:
                idx = node['ModifiedNode']['LedgerIndex']
            elif 'CreatedNode' in node:
                idx = node['CreatedNode']['LedgerIndex']
            else:
                idx = node['DeletedNode']['LedgerIndex']
            objs.add(idx)

    print("Getting ledger objects")
    objsJson = []
    for idx in objs:
        async def getObjs(txWs):
            await txWs.send(json.dumps({"command":"ledger_entry","index":idx,
                "binary":True, "ledger_index":ledgerSeq}))
            return json.loads(await txWs.recv())
        res = await wrap(txAddress, getObjs)
        if 'error' in res:
            assert(res['error'] == 'entryNotFound')
            objsJson.append({"index":idx})
        else:
            objsJson.append({"index":idx,"node_binary":res['result']['node_binary']})


    print("Sending ledger objects")
    async def sendObjs(reportingWs):
        await reportingWs.send(json.dumps({"command": "ledger_accept",
            "load_diff": True, "objs":objsJson}))
        return json.loads(await reportingWs.recv());

    await wrap(reportingAddress, sendObjs)




## Fetch data for all objects in a ledger. Send data to reporting
async def diff_ledgers(txIp, txPort, reportingIp, reportingPort, ledgerSeq):
    txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    ledgerSeq = int(ledgerSeq)
    marker = None
    done = False
    jsonArgs = {"command":"ledger_data","ledger_index":ledgerSeq, "binary": True}

    while not done:
        if marker is not None:
            jsonArgs['marker'] = marker
        payload = None

        async def getData(txWs):
            nonlocal jsonArgs

            await txWs.send(json.dumps(jsonArgs))
            return json.loads(await txWs.recv())

        payload = await wrap(txAddress, getData)
        if 'marker' in payload['result']:
            marker = payload['result']['marker']
        else:
            marker = None
            done = True
        state = payload['result']['state']

        for data in res['state']:
            idx = data['index']
            async def checkReporting(reportingWs):
                await reportingWs.send(json.dumps(
                    {"command": "ledger_entry","index":idx,
                        "binary":True}))
                return json.loads(await reportingWs.recv())['result']
            reportingObj = wrap(reportingAddress, checkReportig)
            if reportingObj['node_binary'] != data['data']:
                print("Diff at ledger object!")
                print(idx)
                return
            print("Match at idx = " + str(idx)) 
    print("All matched!")



parser = argparse.ArgumentParser(description='ETL script for reporting')
parser.add_argument('action', choices=["setup","update","do_all","diff"])
parser.add_argument('--reportingIp', default='127.0.0.1')
parser.add_argument('--reportingPort')
parser.add_argument('--txIp', default='127.0.0.1')
parser.add_argument('--txPort')
parser.add_argument('--ledgerSeq')


args = parser.parse_args()

def run(args):
    asyncio.set_event_loop(asyncio.new_event_loop())
    if args.action == "update":
        asyncio.get_event_loop().run_until_complete(
                update_simple(args.txIp, args.txPort, args.reportingIp,
                    args.reportingPort, args.ledgerSeq))
    elif args.action == "setup":
        asyncio.get_event_loop().run_until_complete(
                setup_simple(args.txIp, args.txPort, args.reportingIp,
                    args.reportingPort, args.ledgerSeq))
    elif args.action == "do_all":
        asyncio.get_event_loop().run_until_complete(
                do_all_simple(args.txIp, args.txPort, args.reportingIp,
                    args.reportingPort, args.ledgerSeq))
    elif args.action == 'diff_ledger':
        asyncio.get_event_loop().run_until_complete(
                diff_ledgers(args.txIp, args.txPort, args.reportingIp,
                    args.reportingPort, args.ledgerSeq))

run(args)

