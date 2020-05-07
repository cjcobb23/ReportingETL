#!/usr/bin/python3

import websockets
import asyncio
import json
import io
import os
import subprocess
import argparse
import time
import threading


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

startSequence = 0
latestSequence = 0
accountTransactions = {}

## Tests that every ledger in the range is validated
async def checkLedgers(cv, address):
    while True:
        with cv:
            cv.wait()
        
        print("checking range " + str(startSequence) + " - " + str(latestSequence))
        for s in range(startSequence, latestSequence):
            async def checkLedger(ws):
                await ws.send(json.dumps({"command":"ledger","ledger_index":s, "transactions":True}))
                res = json.loads(await ws.recv())
                print(res)
                assert res["status"] == "success"
                return await checkTransactions(ws, res["result"]["ledger"]["transactions"])

            res = await wrap(address, checkLedger)

            if not res:
                quit()
        print("successfully verified all ledgers in range " + str(startSequence) + " - " + str(latestSequence))

async def checkAccountTransactions(ws, account):
    print("checkign account transactions for account = " + account)
    marker = None
    hashes = []
    while True:
        print("Sending request")
        if marker:
            await ws.send(json.dumps({"command":"account_tx","account":account, "marker":marker}))
        else:
            await ws.send(json.dumps({"command":"account_tx","account":account}))
        res = json.loads(await ws.recv())
        print("Got response")
        if "error" in res["result"]:
            print("Error getting account transactions. account = " + account)
            return False
        for txn in res["result"]["transactions"]:
            hashes.append(txn["tx"]["hash"])
        print(len(res["result"]["transactions"]))
        if "marker" in res["result"]:
            marker = res["result"]["marker"]
            print(marker)
        else:
            break

    print("num results to account_tx = " + str(len(hashes)))
    for t in accountTransactions[account]:
        if not t in hashes:
            print("Missing hash")
            print(t)
            return False
    print("Finished checking account transactions")
    return True


async def checkTransactions(ws,transactions):
    print("Checking transactions")
    for txID in transactions:
        await ws.send(json.dumps({"command":"tx","transaction":txID}))
        res = json.loads(await ws.recv())
        print(res)
        assert res["status"] == "success"
        account = res["result"]["Account"]
        if account in accountTransactions:
            accountTransactions[account].append(txID)
        else:
            accountTransactions[account] = [txID]
    for account in accountTransactions:
        if not await checkAccountTransactions(ws, account):
            return False
    print("successfully verified transactions")
    return True




async def subscribe(ip, port):

    global startSequence
    global latestSequence
    cv = threading.Condition()

    address = 'ws://' + str(ip) + ':' + str(port)

    def func():
        asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.get_event_loop().run_until_complete(checkLedgers(cv,address))
    t = threading.Thread(target=func)
    t.start()
    try:
        async with websockets.connect(address) as ws:
            await ws.send(json.dumps({"command":"subscribe","streams":["ledger"]}))
            while True:
                payload = json.loads(await ws.recv())
                print(payload)
                if "result" in payload:
                    payload = payload["result"]
                if startSequence == 0:
                    startSequence = payload["ledger_index"]
                latestSequence = payload["ledger_index"]
                with cv:
                    cv.notifyAll()
    except websockets.exceptions.ConnectionClosedError as e:
        print("Websocket closed. Address = " + address)


async def waitForTxn(ws, txID):
    found = False
    numAttempts = 0
    while numAttempts < 10:
        await ws.send(json.dumps({"command":"tx","transaction":txID}))
        res = json.loads(await ws.recv())
        print(res)
        if res["status"] == "success":
            return res["result"]["ledger_index"]
        else:
            numAttempts = numAttempts + 1
            time.sleep(1)
    print("Transction - " + txID + " not confirmed")
    return 0

## Tests that sign and submit work, and that tx and account_tx correctly see the
## submitted transactions
async def test_submit_and_account_tx(ip, port, account, secret, numTxns):
    tx_json = json.loads('{"TransactionType":"Payment", "Amount":"1"}')

    dest = "rMcVp9RLAukieQSZmYKg944kafC2E736ie"
    bad_secret = "DEADBEEF"
    tx_json["Account"] = account;
    tx_json["Destination"] = dest;
    
    address = 'ws://' + str(ip) + ':' + str(port)
    try:
        async with websockets.connect(address) as ws:
            await ws.send(json.dumps({"command":"sign","tx_json":tx_json, "secret":secret}))
            res = json.loads(await ws.recv())
            print(res)
            
            assert res["status"] == "success"
            assert res["forwarded"] == True

            blob = res["result"]["tx_blob"]
            txID = res["result"]["tx_json"]["hash"]
            await ws.send(json.dumps({"command":"submit","tx_blob":blob}))
            res = json.loads(await ws.recv())
            print(res)
            assert res["status"] == "success"

            await ws.send(json.dumps({"command":"sign","tx_json":tx_json, "secret":bad_secret}))
            res = json.loads(await ws.recv())
            print(res)
            assert res["status"] == "error"
            assert res["forwarded"] == True

            assert(await waitForTxn(ws, txID))

            numSent = 0
            txIDs = []
            if numTxns is None:
                numTxns = 10
            else:
                numTxns = int(numTxns)
            while numSent < numTxns:
                time.sleep(.1)
                await ws.send(json.dumps({"command":"sign","tx_json":tx_json, "secret":secret}))
                res = json.loads(await ws.recv())
                print(res)
                if "error" in res and res["error"] == "highFee":
                    time.sleep(4)
                    continue
                
                assert res["status"] == "success"
                assert res["forwarded"] == True

                blob = res["result"]["tx_blob"]
                txID = res["result"]["tx_json"]["hash"]
                txIDs.append(txID)
                await ws.send(json.dumps({"command":"submit","tx_blob":blob}))
                res = json.loads(await ws.recv())
                print(res)
                assert res["status"] == "success"
                time.sleep(.3)

                numSent = numSent+1

            buckets = {}

            ## confirm each tx
            for txID in txIDs:
                lgrIdx = await waitForTxn(ws, txID)
                assert lgrIdx != 0
                if lgrIdx in buckets:
                    buckets[lgrIdx].append(txID)
                else:
                    buckets[lgrIdx] = [txID]




            # Test account_tx

            # Test all txns are there

            async def getAllHashes(ws, account, limit=None, minSeq=None, maxSeq=None):
                hashes = []
                marker = None
                base = {"command":"account_tx","account":account}
                if limit is not None:
                    base["limit"] = limit
                if minSeq is not None and maxSeq is not None:
                    base["ledger_index_min"] = minSeq
                    base["ledger_index_max"] = maxSeq
                while True:
                    if marker:
                        base["marker"] = marker
                        await ws.send(json.dumps(base))
                    else:
                        await ws.send(json.dumps(base))
                    res = json.loads(await ws.recv())
                    assert res["status"] == "success"
                    for txn in res["result"]["transactions"]:
                        hashes.append(txn["tx"]["hash"])
                    if "marker" in res["result"]:
                        marker = res["result"]["marker"]
                        assert len(res["result"]["transactions"]) == 200
                    else:
                        break
                return hashes

            hashes = await getAllHashes(ws, account)

            for txID in txIDs:
                assert txID in hashes

            hashes = await getAllHashes(ws, dest)

            for txID in txIDs:
                assert txID in hashes

            # Test different limits and ranges

            await ws.send(json.dumps({"command":"account_tx","account":account, "limit":0}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert res["result"]["limit"] == 0
            assert len(res["result"]["transactions"]) > 0

            await ws.send(json.dumps({"command":"account_tx","account":account, "limit":1}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert res["result"]["limit"] == 1
            assert len(res["result"]["transactions"]) == 1


            await ws.send(json.dumps({"command":"account_tx","account":account, "limit":1000}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert res["result"]["limit"] == 1000
            assert len(res["result"]["transactions"]) <= 200


            # single ledger
            for seq in buckets:
                await ws.send(json.dumps({"command":"account_tx","account":account, "ledger_index_min":seq,"ledger_index_max":seq}))
                res = json.loads(await ws.recv())
                assert res["status"] == "success"
                assert len(res["result"]["transactions"]) == len(buckets[seq])


            aValue = 0
            for x in buckets:
                aValue = x
                for y in buckets:
                    hashes = await getAllHashes(ws, account, minSeq=min(x,y), maxSeq=max(x,y))
                    exp = 0
                    for z in buckets:
                        if z >= min(x,y) and z <= max(x,y):
                            exp = exp+len(buckets[z])
                    assert len(hashes) == exp
            
            if aValue != 0:
                hashes = await getAllHashes(ws, account, minSeq=0, maxSeq=aValue+1000)
                for txID in txIDs:
                    assert txID in hashes

            hashes = await getAllHashes(ws, account, minSeq=-1, maxSeq=-1)
            for txID in txIDs:
                assert txID in hashes
            print("SUCCESS!")
    except websockets.exceptions.ConnectionClosedError as e:
        print("Websocket closed. Address = " + address)


async def test_proxy(ip, port):
    address = 'ws://' + str(ip) + ':' + str(port)
    try:
        async with websockets.connect(address) as ws:
            await ws.send(json.dumps({"command":"ledger","ledger_index":"current"}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert res["forwarded"] == True
            assert res["result"]["validated"] == False
            assert res["result"]["ledger"]["closed"] == False
            assert int(res["result"]["ledger"]["ledger_index"]) > 100


            await ws.send(json.dumps({"command":"ledger","ledger_index":"closed"}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert res["forwarded"] == True
            assert res["result"]["ledger"]["closed"] == True
            assert int(res["result"]["ledger"]["ledger_index"]) > 100


            await ws.send(json.dumps({"command":"ledger","ledger_index":"validated"}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert not "forwarded" in res
            assert res["result"]["validated"] == True
            assert res["result"]["ledger"]["closed"] == True
            assert int(res["result"]["ledger"]["ledger_index"]) > 100


            await ws.send(json.dumps({"command":"fee"}))
            res = json.loads(await ws.recv())
            assert res["status"] == "success"
            assert res["forwarded"] == True
            assert int(res["result"]["ledger_current_index"]) > 100
            print("SUCCESS")
    except websockets.exceptions.ConnectionClosedError as e:
        print("Websocket closed. Address = " + address)







            #assert(await waitForTxn(ws, txID))

##            for ID in txIDs:
##                await ws.send(json.dumps({"command":"tx","transaction":ID}))
##                res = json.loads(await ws.recv())
##                print(res)
##                assert res["status"] == "success"
                
    except websockets.exceptions.ConnectionClosedError as e:
        print("Websocket closed. Address = " + address)

async def test_tx(ip, port):
    address = 'ws://' + str(ip) + ':' + str(port)
    try:
        while True:
            async with websockets.connect(address) as ws:
                await ws.send(json.dumps({"command":"ledger","ledger_index":"validated", "transactions":True}))
                res = json.loads(await ws.recv())
                assert res["status"] == "success"
                idx = res["result"]["ledger_index"]
                transactions = res["result"]["ledger"]["transactions"]
                print(transactions)
                if len(transactions) == 0:
                    print("ledger has no txns")
                    continue
                txID = transactions[0]
                await ws.send(json.dumps({"command":"tx", "transaction":txID}))
                res = json.loads(await ws.recv())
                assert not "error" in res["result"]
                print(res)
                assert res["result"]["ledger_index"] == idx
                assert res["result"]["validated"]


                await ws.send(json.dumps({"command":"tx", "transaction":txID, "binary":True}))
                res = json.loads(await ws.recv())
                assert not "error" in res["result"]
                print(res)
                assert res["result"]["ledger_index"] == idx
                assert res["result"]["validated"]

                txID = "E08D6E9754025BA2534A78707605E0601F03ACE063687A0CA1BDDACFCD1698C7"
                
                await ws.send(json.dumps({"command":"tx", "transaction":txID}))
                res = json.loads(await ws.recv())
                print(res)
                assert "error" in res
                assert res["error"] == "txnNotFound"
                assert not "searched_all" in res

                await ws.send(json.dumps({"command":"tx", "transaction":txID, "min_ledger":idx-1,"max_ledger":idx}))
                res = json.loads(await ws.recv())
                
                print(res)
                assert "error" in res
                assert res["error"] == "txnNotFound"
                assert "searched_all" in res
                assert res["searched_all"] == True
                
                await ws.send(json.dumps({"command":"tx", "transaction":txID, "min_ledger":idx-1,"max_ledger":idx+1000}))
                res = json.loads(await ws.recv())

                print(res)
                assert "error" in res
                assert res["error"] == "txnNotFound"
                assert "searched_all" in res
                assert res["searched_all"] == False

                await ws.send(json.dumps({"command":"tx", "transaction":txID, "min_ledger":2,"max_ledger":idx}))
                res = json.loads(await ws.recv())

                print(res)
                assert "error" in res
                assert res["error"] == "txnNotFound"
                assert "searched_all" in res
                assert res["searched_all"] == False
                print("SUCCESS!")
                break
    except websockets.exceptions.ConnectionClosedError as e:
        print("Websocket closed. Address = " + address)
            








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
            print("sending data to reporting. marker = " + str(marker))
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
        print(res)
        if 'msg' in res['result'] or (res['result']['account_hash'] == 'correct' and res['result']['tx_hash'] == 'correct'):
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
            if res['error'] != 'entryNotFound':
                print(res)
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
parser.add_argument('action', choices=["subscribe","diff_ledger", "tx","submit","proxy"])
parser.add_argument('--reportingIp', default='127.0.0.1')
parser.add_argument('--reportingPort')
parser.add_argument('--txIp', default='127.0.0.1')
parser.add_argument('--txPort')
parser.add_argument('--ledgerSeq')
parser.add_argument('--account')
parser.add_argument('--secret')
parser.add_argument('--numTxns')


args = parser.parse_args()

def run(args):
    asyncio.set_event_loop(asyncio.new_event_loop())
    if args.action == "subscribe":
        asyncio.get_event_loop().run_until_complete(
                subscribe(args.reportingIp, args.reportingPort))
    elif args.action == "tx":
        asyncio.get_event_loop().run_until_complete(
                test_tx(args.reportingIp, args.reportingPort))
    elif args.action == "submit":
        asyncio.get_event_loop().run_until_complete(
            test_submit_and_account_tx(args.reportingIp, args.reportingPort, args.account, args.secret, args.numTxns))
    elif args.action == "proxy":
        asyncio.get_event_loop().run_until_complete(
            test_proxy(args.reportingIp, args.reportingPort))
    elif args.action == 'diff_ledger':
        asyncio.get_event_loop().run_until_complete(
                diff_ledgers(args.txIp, args.txPort, args.reportingIp,
                    args.reportingPort, args.ledgerSeq))

run(args)

