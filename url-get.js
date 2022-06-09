import { ListObjectsV2Command } from '@aws-sdk/client-s3'

import { getReadContext } from './context.js'
import { BackfillState } from './state.js'

/**
 * 
 * @param {{ stateDB?: string|null, startDate: string, endDate: string }} opts 
 */
 export async function getAllBackupUrls(opts) {
    const context = await getReadContext()
  
    const startDate = new Date(opts.startDate)
    const endDate = new Date(opts.endDate)

    const state = await BackfillState.open({ filename: opts.stateDB })
  
    console.log('Querying db for affected uploads...')
    const uploads = await findUploadsWithMissingURLS(context, startDate, endDate)
    console.log(`Found ${uploads.length} uploads to check.`)

    for (const u of uploads) {
        await state.addCandidate(u)
    }

    let numChecked = 0
    const logFreq = 1000

    console.log('recording backup urls to state db at ' + state.filename)
    console.log('checking s3 to discover backup urls...')
    const unchecked = await state.getUncheckedCandidates()
    for (const upload of unchecked) {
      const urls = await getBackupURLsFromS3(
        context,
        upload.source_cid,
        upload.user_id
      )
      await state.addDiscoveredUrls(upload.id, urls)
      numChecked += 1
  
      if ((numChecked % logFreq) == 0) {
        console.log(`checked ${numChecked} / ${uploads.length}`)
      }
    }

    console.log('Backup URL discovery complete.')
    await state.close()
  }
  
  /**
   * Queries the database to find uploads that may be missing backup_urls.
   *
   * Assumes that any upload with an `updated_at` > `inserted_at` is potentially
   * a chunked upload and may be missing one or more backup urls. Since the uploads
   * affected by the bug all have a single `backup_url`, we can further filter out
   * any rows with more than one `backup_url`.
   *
   * @param {Context} context
   * @param {Date} startDate
   * @param {Date} endDate
   *
   * @typedef {object} ResultRow
   * @property {string} id
   * @property {string} source_cid
   * @property {string} user_id
   *
   * @returns {Promise<ResultRow[]>}
   */
  async function findUploadsWithMissingURLS(context, startDate, endDate) {
    const { db } = context
  
    console.log('querying db. start / end dates: ', startDate, endDate)
  
    const res = await db.query(
      'SELECT id, source_cid, user_id FROM upload' +
        ' WHERE updated_at >= $1 AND updated_at <= $2' +
        ' AND updated_at > inserted_at ' +
        ' AND cardinality(backup_urls) < 2' +
        ' ORDER BY updated_at DESC' +
        ';',
      [startDate, endDate]
    )
  
    return res.rows
  }
  
  /**
   *
   * @param {Context} context
   * @param {string} sourceCid
   * @param {string} userId
   * @param {string} appName - name of uploading app. always "nft" for production uploads
   *
   * @returns {Promise<URL[]>} all discovered backup URLs for the given cid+userId
   */
  async function getBackupURLsFromS3(
    context,
    sourceCid,
    userId,
    appName = 'nft'
  ) {
    const uploadDirPrefix = `raw/${sourceCid}/${appName}-${userId}/`
  
    const urls = []
    for await (const key of listObjects(context, uploadDirPrefix)) {
      urls.push(new URL(key, context.s3BaseURL.toString()))
    }
  
    return urls
  }
  
  /**
   *
   * @param {Context} context
   * @param {string} prefix - prefix (directory path) to list
   * @param {string|undefined} continuationToken - used to recursively fetch objects if more than 1000 keys match
   *
   * @returns {AsyncGenerator<string>} - yields the full key of each object found under the given prefix
   */
  async function* listObjects(context, prefix, continuationToken = undefined) {
    const cmd = new ListObjectsV2Command({
      Bucket: context.s3BucketName,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    })
    /** @type {import('@aws-sdk/client-s3').ListObjectsV2CommandOutput} */
    const response = await context.s3.send(cmd)
    const contents = response.Contents || []
    for (const obj of contents) {
      if (obj.Key) {
        yield obj.Key
      }
    }
  
    if (response.IsTruncated && response.NextContinuationToken) {
      for await (const key of listObjects(
        context,
        prefix,
        response.NextContinuationToken
      )) {
        yield key
      }
    }
  }
  
