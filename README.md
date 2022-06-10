# backfill-s3-urls

A utility to backfill missing backup_url entries for uploads that were affected by https://github.com/nftstorage/nft.storage/issues/1939

## usage

First install dependencies:

```
npm install
```

Create a file called `.env` that looks like this, filling in the values:

```
RO_DATABASE_CONNECTION=<connection-uri-for-readonly-postgres>
DATABASE_CONNECTION=<connection-uri-for-db-to-update>

S3_ACCESS_KEY_ID=<your-aws-key-id>
S3_SECRET_ACCESS_KEY=><your-aws-key>
S3_BUCKET_NAME=dotstorage-prod-0
S3_REGION=us-east-2
```


Backfilling is a two-step process. 

### Step one: Dumping the backup urls

In step one, the `get-urls` command connects to a read-only DB replica to find "candidate" uploads that might need backfilling.
It then constructs the object prefix for each candidate's backups on S3 and lists CAR files matching the prefix.

For each candidate upload, we construct the expected S3 object prefix and query for CAR files.

The upload ids and discovered urls are recorded to a sqlite "state db" file, which gets passed in to the `put-urls` command
in step 2.

Example:

```
node ./index.js get-urls
```

You can also set the start and end dates, and control where the state db file will be written. Use `node ./index.js get-urls --help` for options.

At the end, you should have a file named `backfill-${startDate}-${endDate}.db` in your current directory.

### Step two: Writing the urls to the database

Once you've run `get-urls`, you can run the `update-urls` command to update the nft.storage DB with the urls discovered in step one.

Example:

```
node ./index.js update-urls --stateDB=<path-to-state-db-file-from-step-one>
```

The urls will be updated in batches, with a sleep interval between batches. Use `node ./index.js update-urls --help` for options.
