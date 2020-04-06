# ReportingETL

This code is a python script that fetches data from a rippled node, and feeds
the data to a rippled in reporting mode. Reporting mode does not yet exist, but
a prototyped reporting mode lives under the branch
[reportingModeV3](https://github.com/cjcobb23/rippled/tree/reportingModeV3).

To run:
1. build rippled and run in normal mode.
2. Build the reportingModeV3 branch, and run in standalone mode.Now there are two rippleds running
3. Run `python3 etl.py do_all --txPort 6010 --reportingPort 6006`, where reporting
mode rippled is listening on port 6006 for websocket requests, and regular
rippled is listening on port 6010 for websocket requests.

The script will first download the latest validated ledger. This can take
minutes, and takes substantially longer on mainnet than testnet. You will see
many print statements saying "Sending data to reporting. marker = [marker]".
The marker should be constantly changing. Once all data is downloaded, the
script tells reporting mode to recompute the hashes and save the data; reporting
mode will respond with a message that says whether the computed account hash
matches the account hash provided in the ledger header. This step can be run
independently, substituting `setup` for `do_all`.

Then, the script enters a loop where it fetches the next ledger, with its header
and tx meta data. From the metadata, the script determines which objects were
modified as part of this ledger. The script then fetches each of those objects
in its entirety, and sends the blobs to reporting. Once all blobs have been
ingested, the script instructs reporting to save the ledger, verifying the
hashes are correct. This step can be run independently (as a loop), by
substituting `update` for `do_all`. However, when running this step independently, 
the ledger sequence must be passed via the command line as an integer.

The ledger sequence can be passed via the command line for `do_all` and `setup`
as well, but will default to `validated` if not passed. For `update`, the ledger
sequence must be specified as an integer.
