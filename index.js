
import sade from 'sade'
import dotenv from 'dotenv'

import { getAllBackupUrls } from './url-get.js'
import { updateBackupUrls } from './url-put.js'

dotenv.config()

/**
 * This script sets the `backup_urls` field for an upload by checking the storage on S3.
 *
 * See https://github.com/nftstorage/nft.storage/issues/1939 for motivation. TL;DR is that
 * a bug was causing us to only keep one backup url for chunked CAR uploads.
 */

const prog = sade('backfill-s3-urls')
prog
  .command('get-urls')
    .option('startDate', 'start date of query. must be parseable by `new Date()`', '2022-03-17')
    .option('endDate', 'end date of query. must be parseable by `new Date()`', '2022-06-05')
    .option('skipDBQuery', 'skip querying for candidates (only useful if re-using stateDB from prior run', false)
    .option(
      'stateDB', 
      'path to local state database that records intermediate data and backfill progress. ' +
      'If not given, outputs state db file in current dir based on start/end dates', 
      null)
  .action(getAllBackupUrls)

  .command('update-urls')
    .option('stateDB', 'path to state DB file produced by get-urls')
    .option('batchSize', 'number of uploads to update in each batch', 300)
    .option('interval', 'time (in seconds) to wait between batches', 10)
    .action(updateBackupUrls)

prog.parse(process.argv)


