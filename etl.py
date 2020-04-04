#!/usr/bin/python3

import websockets
import asyncio
import json
import io
import os
import subprocess
import argparse
import time


async def coro():
    async with websockets.connect("ws://127.0.0.1:6006", ping_interval=5, ping_timeout=3) as websocket:
        await websocket.send(json.dumps({'command':'subscribe', 'streams':['ledger']}))

        res = json.loads(await websocket.recv())['result']
        print(res)
        while True:
            res = json.loads(await websocket.recv())
            ledger_index = res['ledger_index']
            print(res)
            async with websockets.connect("ws://127.0.0.1:6006", ping_interval=5, ping_timeout=3) as websocket2:
                await websocket2.send(json.dumps({"command":"ledger","ledger_index":ledger_index, 'transactions': True, 'expand': True, 'binary':True}))
                res2 = json.loads(await websocket2.recv())['result']
                print(res2)
                transactions = res2['ledger']['transactions']
                async with websockets.connect("ws://127.0.0.1:6007", ping_interval=5, ping_timeout=3) as websocket3:

                    for t in transactions:
                        print(t['tx_blob'])
                        await websocket3.send(json.dumps({"command":"submit",'tx_blob':t['tx_blob']}))
                        print(json.loads(await websocket3.recv()))
                    await websocket3.send(json.dumps({"command":"ledger_accept"}))
                    print(json.loads(await websocket3.recv()))

                print("*****************")


async def get_subscribe(txIp, txPort, reportingIp, reportingPort):
    #txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    txAddress = 'wss://s.altnet.rippletest.net/'
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")
    async with websockets.connect(reportingAddress, ping_interval=5, ping_timeout=3) as reportingWs:
        print("connected to reporting")
        ready = False
        seq = 0
        # wait for the reporting process to be available. It may be in the
        # middle of loading the database
        while not ready:
            await reportingWs.send(json.dumps({'command':'server_info'}))
            res = json.loads(await reportingWs.recv())
            if 'result' in res and 'validated_ledger' in res['result']['info']:
                seq = res['result']['info']['validated_ledger']['seq']
                ready = True

        seq = seq + 1
        print("Seq is = " + str(seq))
        print("Connecting to tx")
        async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txSubWs:
            print("connected to tx")


            await txSubWs.send(json.dumps({'command':'subscribe', 'streams':['ledger']}))


            res = json.loads(await txSubWs.recv())['result']
            print(res)
            async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:
                while True:
                    res = json.loads(await txSubWs.recv())
                    ledger_index = res['ledger_index']
                    close_time = res['ledger_time']
                    while seq <= ledger_index:


                        await txWs.send(json.dumps({"command":"ledger","ledger_index":seq, 'transactions': True, 'expand': True, 'binary':True}))
                    # get ledgers in increasing order. If ledger is not available, sleep
                    # for two seconds and try again
                        res = json.loads(await txWs.recv())
                        if 'error' in res or res['result']['validated'] == False:
                            print("Ledger not yet validated. Sleeping...")
                            #await asyncio.sleep(2)
                            continue
                        else:
                            print('Ledger ' + str(seq) + 'validated. Importing...')

                        print("***")
                        print(res)
                        print("***")
                        lgr = res['result']
                        print(lgr)
                        transactions = lgr['ledger']['transactions']

                        # submit each transaction to reporting
                        for t in transactions:
                            await reportingWs.send(json.dumps({"command":"submit",'tx_blob':t['tx_blob']}))
                            res = json.loads(await reportingWs.recv())
                            print(res)
                            result = res['result']
                            print(result['tx_json']['hash'])
                            print('engine_result = '
                                    + result['engine_result']
                                    + " . engine_result_message = "
                                    + result['engine_result_message'])

                        # close the ledger
                        await reportingWs.send(json.dumps({"command":"ledger_accept", 'close_time':close_time, "ledger_index":seq}))
                        print(json.loads(await reportingWs.recv()))
                        await reportingWs.send(json.dumps({'command':'ledger', 'ledger_index':seq}))
                        reportingLgr = json.loads(await reportingWs.recv())['result']

                        if lgr['ledger_hash'] != reportingLgr['ledger_hash']:
                            print("ledger hash mismatch! aborting")
                            print(reportingLgr)
                            print("*****")
                            print(lgr)
                            return
                        else:
                            print("Successfully imported ledger! Seq = " + str(seq))
                        seq = seq + 1






















async def get_increasing(txIp, txPort, reportingIp, reportingPort):
    txAddress = None
    if txPort is None or txIp is None:
        txAddress = 'wss://s.altnet.rippletest.net/'
    else:
        txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")
    async with websockets.connect(reportingAddress, ping_interval=5, ping_timeout=3) as reportingWs:
        print("connected to reporting")
        ready = False
        seq = 0
        # wait for the reporting process to be available. It may be in the
        # middle of loading the database
        while not ready:
            await reportingWs.send(json.dumps({'command':'server_info'}))
            res = json.loads(await reportingWs.recv())
            if 'result' in res and 'validated_ledger' in res['result']['info']:
                seq = res['result']['info']['validated_ledger']['seq']
                ready = True

        seq = seq + 1
        print("Seq is = " + str(seq))
        print("Connecting to tx")
        async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:
            print("connected to tx")
            # get ledgers in increasing order. If ledger is not available, sleep
            # for two seconds and try again
            while True:
                await txWs.send(json.dumps({"command":"ledger","ledger_index":seq}))
                res = json.loads(await txWs.recv())
                if 'error' in res or res['result']['validated'] == False:
                    print("Ledger not yet validated. Sleeping...")
                    await asyncio.sleep(2)
                    continue
                else:
                    print('Ledger ' + str(seq) + 'validated. Importing...')

                close_time = res['result']['ledger']['close_time']

                await txWs.send(json.dumps({"command":"ledger","ledger_index":seq, 'transactions': True, 'expand': True, 'binary':True}))
                res = json.loads(await txWs.recv())
                print(res)
                lgr = res['result']
                transactions = lgr['ledger']['transactions']
                # submit each transaction to reporting
                for t in transactions:
                    await reportingWs.send(json.dumps({"command":"submit",'tx_blob':t['tx_blob']}))
                    res = json.loads(await reportingWs.recv())
                    print(res)
                    result = res['result']
                    print(result['tx_json']['hash'])
                    print('engine_result = '
                            + result['engine_result']
                            + " . engine_result_message = "
                            + result['engine_result_message'])

                # close the ledger
                await reportingWs.send(json.dumps({"command":"ledger_accept", 'close_time':close_time, "ledger_index":seq}))
                print(json.loads(await reportingWs.recv()))
                await reportingWs.send(json.dumps({'command':'ledger', 'ledger_index':seq}))
                reportingLgr = json.loads(await reportingWs.recv())['result']

                if lgr['ledger_hash'] != reportingLgr['ledger_hash']:
                    print("ledger hash mismatch! aborting")
                    print(reportingLgr)
                    print("*****")
                    print(lgr)
                    return
                else:
                    print("Successfully imported ledger! Seq = " + str(seq))
                seq = seq + 1


async def load_txs(txIp, txPort, reportingIp, reportingPort, seq):
    txAddress = None
    if txPort is None or txIp is None:
        txAddress = 'wss://s.altnet.rippletest.net/'
    else:
        txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")
    async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:
        await txWs.send(json.dumps({"command":"ledger","ledger_index":int(seq), 'transactions': True, 'expand': True, 'binary':True}))
        txs = json.loads(await txWs.recv())['result']['ledger']['transactions']
        print(txs)
        async with websockets.connect(reportingAddress) as reportingWs:
            await reportingWs.send(json.dumps({"command": "ledger_accept",
                "load_meta": True, "transactions": txs}))
            res = json.loads(await reportingWs.recv());
            print(res)




async def load_diff(txIp, txPort, reportingIp, reportingPort, ledgerIndex):
    txAddress = None
    if txPort is None or txIp is None:
        txAddress = 'wss://s.altnet.rippletest.net/'
    else:
        txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to tx")

    async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:
        await txWs.send(json.dumps(
            {"command":"ledger",
                "ledger_index": int(ledgerIndex) if ledgerIndex is not None else "validated",
                'transactions': True, 'expand': True}))
        res = json.loads(await txWs.recv())
        print(res)
        ledgerIndex = res['result']['ledger_index']
        
        objs = set()


        for tx in res['result']['ledger']['transactions']:
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

        print(objs)
        objsJson = []
        for idx in objs:
            await txWs.send(json.dumps({"command":"ledger_entry","index":idx,"binary":True, "ledger_index":ledgerIndex}))
            res = json.loads(await txWs.recv())
            if 'error' in res:
                assert(res['error'] == 'entryNotFound')
                objsJson.append({"index":idx})
            else:
                objsJson.append({"index":idx,"node_binary":res['result']['node_binary']})
        print(objsJson)

        async with websockets.connect(reportingAddress) as reportingWs:
            await reportingWs.send(json.dumps({"command": "ledger_accept",
                "load_diff": True, "objs":objsJson}))
            res = json.loads(await reportingWs.recv());
            print(res['result']['msg'])

async def load_with_data(txIp, txPort, reportingIp, reportingPort, seq):
    while True:
        seq = await load_ledger(txIp, txPort, reportingIp, reportingPort, seq)
        await load_diff(txIp, txPort, reportingIp, reportingPort, seq)
        res = await finish(txIp, txPort, reportingIp, reportingPort)
        if res['result']['account_hash'] == 'correct' and res['result']['tx_hash'] == 'correct':
            print("successfuly loaded ledger = " + str(seq))
            seq = seq + 1
        else:
            print("Failed to load ledger = " + str(seq))
            break
async def do_all(txIp, txPort, reportingIp, reportingPort, seq):
    seq = await load_ledger(txIp, txPort, reportingIp, reportingPort, seq)
    await load_data(txIp, txPort, reportingIp, reportingPort, seq)
    res = await finish(txIp, txPort, reportingIp, reportingPort)
    if res['result']['account_hash'] == 'correct' and res['result']['tx_hash'] == 'correct':
        print("successfuly loaded ledger = " + str(seq))
        seq = seq + 1
    else:
        print("Failed to load ledger = " + str(seq))
        return
    await load_with_data(txIp, txPort, reportingIp, reportingPort, seq)

async def load_ledger(txIp, txPort, reportingIp, reportingPort, seq):
    #txAddress = 'ws://' + str(txIp) + ':' + str(txPort)

    txAddress = None
    if txPort is None or txIp is None:
        txAddress = 'wss://s.altnet.rippletest.net/'
    else:
        txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")

    async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:

        res = None
        index = "validated"
        if seq is not None:
            index = int(seq)
        while True:
            await txWs.send(json.dumps({"command":"ledger","ledger_index":index}))
            payload = json.loads(await txWs.recv())
            if 'error' in payload or not payload['result']['validated']:
                print("waiting for ledger : " + str(index))
                await asyncio.sleep(2)
                continue
            print(payload)
            res = payload['result']['ledger']
            print(res)
            seq = res['ledger_index']
            break
        
        txs = None
        while True:
            await txWs.send(json.dumps({"command":"ledger","ledger_index":int(seq), 'transactions': True, 'expand': True, 'binary':True}))
            payload = json.loads(await txWs.recv())
            if 'error' in payload:
                print("Waiting for ledger : " + seq)
                await asyncio.sleep(2)
                continue
            else:
                txs = payload['result']['ledger']['transactions']
                break

        async with websockets.connect(reportingAddress) as reportingWs:
            await reportingWs.send(json.dumps({"command": "ledger_accept","ledger":res}))
            res = json.loads(await reportingWs.recv());
            print(res)
            await reportingWs.send(json.dumps({"command": "ledger_accept",
                "load_txns": True, "transactions": txs}))
            res = json.loads(await reportingWs.recv());
            print(res)
    return int(seq)



async def diff_ledgers(txIp, txPort, reportingIp, reportingPort, ledgerSeq):
    txAddress = None
    if txPort is None or txIp is None:
        txAddress = 'wss://s.altnet.rippletest.net/'
    else:
        txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")

    marker = None
    done = False
    closed = False
    while not done:
        try:
            async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:
                async with websockets.connect(reportingAddress, ping_interval=5, ping_timeout=3) as reportingWs:
                    while not done:
                        if marker is None:
                            print("sending without marker")
                            await txWs.send(json.dumps({"command":"ledger_data","ledger_index":int(ledgerSeq), "binary": True}))
                        else:
                            print("sending with marker")
                            await txWs.send(json.dumps({"command":"ledger_data",'marker':marker,"ledger_index":int(ledgerSeq), "binary": True}))
                        result = json.loads(await txWs.recv())
                        res = result['result']
                        if 'marker' in res:
                            marker = res['marker']
                        else:
                            print("Done downloading data")
                            marker = None
                            done = True
                        print(marker)
                        for data in res['state']:
                            idx = data['index']
                            await reportingWs.send(json.dumps({"command": "ledger_entry","index":idx, "binary":True}))

                            reportingObj = json.loads(await reportingWs.recv())['result']
                            if reportingObj['node_binary'] != data['data']:
                                print("Diff at ledger object!")
                                print(idx)
                                return
        except websockets.exceptions.ConnectionClosedError as e:
            print("Websocket closed. Sleeping and reconnecting")
            closed = True
            await asyncio.sleep(2)




async def load_data(txIp, txPort, reportingIp, reportingPort, ledgerSeq):
    txAddress = None
    if txPort is None or txIp is None:
        txAddress = 'wss://s.altnet.rippletest.net/'
    else:
        txAddress = 'ws://' + str(txIp) + ':' + str(txPort)
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")

    marker = None
    done = False
    closed = False
    while not done:
        try:
            async with websockets.connect(txAddress, ping_interval=5, ping_timeout=3) as txWs:
                async with websockets.connect(reportingAddress, ping_interval=5, ping_timeout=3) as reportingWs:
                    while not done:
                        if marker is None:
                            print("sending without marker")
                            await txWs.send(json.dumps({"command":"ledger_data","ledger_index":int(ledgerSeq), "binary": True}))
                        else:
                            print("sending with marker")
                            await txWs.send(json.dumps({"command":"ledger_data",'marker':marker,"ledger_index":int(ledgerSeq), "binary": True}))
                        result = json.loads(await txWs.recv())
                        res = result['result']
                        if marker is not None and not closed:
                            #pass
                            await reportingWs.recv()
                        if 'marker' in res:
                            marker = res['marker']
                        else:
                            print("Done downloading data")
                            marker = None
                            done = True
                        print(marker)
                        await reportingWs.send(json.dumps({"command": "ledger_accept","ledger_data":"","state":res['state']}))
                        if done == True:
                            await reportingWs.recv()
        except websockets.exceptions.ConnectionClosedError as e:
            print("Websocket closed. Sleeping and reconnecting")
            closed = True
            await asyncio.sleep(2)


async def finish(txIp, txPort, reportingIp, reportingPort):
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")
    async with websockets.connect(reportingAddress) as reportingWs:
        await reportingWs.send(json.dumps({"command": "ledger_accept","finish":True}))
        res = json.loads(await reportingWs.recv());
        print(res)
        return res

async def accept(reportingIp, reportingPort, ledgerIndex, closeTime):
    reportingAddress = 'ws://' + str(reportingIp) + ':' + str(reportingPort)
    print("connecting to reporting")
    async with websockets.connect(reportingAddress, ping_interval=5, ping_timeout=3) as reportingWs:
        await reportingWs.send(json.dumps({"command":"ledger_accept", 'close_time':int(closeTime), "ledger_index":int(ledgerIndex)}))
        res = json.loads(await reportingWs.recv());
        print(res)



async def wait_until_synced(ip, port):
    address = 'ws://' + ip + ':' + port
    print("Connecting to " + address)
    attempts = 0
    # if we can't connect to the server after 20 seconds, abort
    while attempts < 5:
        try:
            async with websockets.connect(address, ping_interval=5, ping_timeout=3) as reportingWs:
                print("Connected to " + address)
                synced = False
                while not synced:
                    await asyncio.sleep(5)
                    await reportingWs.send(json.dumps({'command':'server_info'}))
                    server_info = json.loads(await reportingWs.recv())['result']
                    state = server_info['info']['server_state']
                    if state == "full" or state == "proposing" or state == "validating" :
                        seq = server_info['info']['validated_ledger']['seq']
                        print("synced! sequence = " + str(seq))
                        synced = True
                    else:
                        print("Not yet synced...")

                return seq
        except ConnectionRefusedError as e:
            print("Connection to " + address + " refused. Trying again in 4 seconds")
            attempts = attempts + 1
            await asyncio.sleep(4)
    return None

async def stop(ip, port):
    address = 'ws://' + ip + ':' + port
    attempts = 0
    # if we can't connect to the server after 3 attempts, abort
    while attempts < 3:
        try:
            async with websockets.connect(address, ping_interval=5, ping_timeout=3) as reportingWs:
                await reportingWs.send(json.dumps({'command':'stop'}))
                print(json.loads(await reportingWs.recv()))
                return True
        except ConnectionRefusedError as e:
            print("refused")
            attempts = attempts + 1
            await asyncio.sleep(1)
    return None

def restart_in_standalone(buildDir, conf):
    #os.system('cd ' + buildDir)
    if os.system(buildDir + '/rippled -a --load --conf ' + conf + ' > /dev/null 2>&1 &') == 0:
        print("Started in standalone mode")
    else:
        print("Error starting rippled in standalone mode")
    #os.system('cd -')


def start_in_daemon(buildDir, conf):
    #os.system('cd ' + buildDir)
    if os.system(buildDir + '/rippled --conf ' + conf + ' > /dev/null 2>&1 &') == 0:
        print("Started in daemon mode")
    else:
        print("Error starting rippled in daemon mode")
    #os.system('cd -')

parser = argparse.ArgumentParser(description='ETL script for transactions')
parser.add_argument('action', choices=['sync','accept','etl','sub','restart','load','data','finish','txs','diff','all','load_txs_finish',"load_diff", "go", "do_all"])
parser.add_argument('--buildDir', default='~/Code/rippled/build')
parser.add_argument('--reportingIp', default='127.0.0.1')
parser.add_argument('--reportingPort', default='6007')
parser.add_argument('--txIp', default='127.0.0.1')
parser.add_argument('--txPort')
parser.add_argument('--confReporting', default='~/.config/ripple/rippled2.cfg')
parser.add_argument('--confTx', default='~/.config/ripple/rippled.cfg')
parser.add_argument('--ledgerSeq')
parser.add_argument('--closeTime')


args = parser.parse_args()

def run(args):
    asyncio.set_event_loop(asyncio.new_event_loop())
    if args.action == 'sync' :
        start_in_daemon(args.buildDir, args.reportingConf)
        res = asyncio.get_event_loop().run_until_complete(wait_until_synced(args.reportingIp, args.reportingPort))
        if res is None:
            print("Failed to connect to rippled server")
        print("Synced successfully!")
    elif args.action == 'restart':
        asyncio.get_event_loop().run_until_complete(stop(args.reportingIp, args.reportingPort))
        restart_in_standalone(args.buildDir, args.confReporting)
    elif args.action == 'etl':
        while True:
            try:
                asyncio.get_event_loop().run_until_complete(get_increasing(args.txIp, args.txPort, args.reportingIp, args.reportingPort))
                break
            except websockets.exceptions.ConnectionClosedError as e:
                print("Connection closed. Sleeping...")
                time.sleep(20)
                print("Trying again")
    elif args.action == "go":
        asyncio.get_event_loop().run_until_complete(load_with_data(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == "do_all":
        asyncio.get_event_loop().run_until_complete(do_all(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == 'sub':
        asyncio.get_event_loop().run_until_complete(get_subscribe(args.txIp, args.txPort, args.reportingIp, args.reportingPort))
    elif args.action == 'load_diff':
        asyncio.get_event_loop().run_until_complete(load_diff(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == 'load':
        asyncio.get_event_loop().run_until_complete(load_ledger(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == 'diff':
        asyncio.get_event_loop().run_until_complete(diff_ledgers(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == 'data':
        asyncio.get_event_loop().run_until_complete(load_data(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == 'txs':
        asyncio.get_event_loop().run_until_complete(load_txs(args.txIp, args.txPort, args.reportingIp, args.reportingPort, args.ledgerSeq))
    elif args.action == 'finish':
        asyncio.get_event_loop().run_until_complete(finish(args.txIp, args.txPort, args.reportingIp, args.reportingPort))
    elif args.action == 'accept':
        asyncio.get_event_loop().run_until_complete(accept(args.reportingIp, args.reportingPort, args.ledgerSeq, args.closeTime))
    elif args.action == "load_txs_finish":
        ledgerSeq = args.ledgerSeq
        while True:
            asyncio.get_event_loop().run_until_complete(load_ledger(args.txIp, args.txPort, args.reportingIp, args.reportingPort, ledgerSeq))

            asyncio.get_event_loop().run_until_complete(load_txs(args.txIp, args.txPort, args.reportingIp, args.reportingPort, ledgerSeq))
            asyncio.get_event_loop().run_until_complete(finish(args.txIp, args.txPort, args.reportingIp, args.reportingPort))
            ledgerSeq = int(ledgerSeq) + 1
    elif args.action == 'all':
        # shutdown all rippleds
        os.system('pkill rippled')
        # start two rippleds
        start_in_daemon(args.buildDir, args.confTx)
        start_in_daemon(args.buildDir, args.confReporting)
        # wait until reporting process is synced to the network
        # TODO this should use the import semantics, but I can't figure out how
        # to get import to work with standalone mode
        res = asyncio.get_event_loop().run_until_complete(wait_until_synced(args.reportingIp, args.reportingPort))
        if res is None:
            print("Failed to connect to reporting rippled server")
            return
        print("Synced successfully!")
        # shut down reporting process
        asyncio.get_event_loop().run_until_complete(stop(args.reportingIp, args.reportingPort))
        # bring reporting backup in standalone. will load ledger from db
        restart_in_standalone(args.buildDir, args.confReporting)
        # make sure tx process is synced to network
        res = asyncio.get_event_loop().run_until_complete(wait_until_synced(args.txIp, args.txPort))
        if res is None:
            print("Failed to connect to tx rippled server")
            return
        # import ledgers from tx to reporting
        asyncio.get_event_loop().run_until_complete(get_increasing(args.txIp, args.txPort, args.reportingIp, args.reportingPort))

run(args)

#asyncio.get_event_loop().run_until_complete(coro())

#asyncio.get_event_loop().run_until_complete(wait_until_synced())

#restart_in_standalone()

#await asyncio.sleep(5)



