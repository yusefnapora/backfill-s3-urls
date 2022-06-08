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
S3_ACCESS_KEY_ID=<your-aws-key-id>
S3_SECRET_ACCESS_KEY=><your-aws-key>
S3_BUCKET_NAME=dotstorage-prod-0
S3_REGION=us-east-2
```

### Dumping the backup urls

Use the `get-urls` command in `index.js` to query the database for uploads that may be missing backup urls.

For each candidate upload, we construct the expected S3 object prefix and query for CAR files. If there's more than one, we record the upload id and backup urls in a big newline-delimited JSON file.

Example:

```
node ./index.js get-urls
```

You can also set the start and end dates, and control where the json file will be written. Use `node ./index.js get-urls --help` for options.

### Writing the urls to the database

TBD / in progress.

The plan is to partition the big ndjson file from `get-urls` into small batches and update the db slowly over time, to avoid putting undue load on the production DB or locking the uploads table.
