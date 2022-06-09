import { S3Client } from '@aws-sdk/client-s3'

import { getDbClient } from './db.js'


  /**
   * @typedef {object} ReadContext
   * @property {import('pg').Client} db
   * @property {import('@aws-sdk/client-s3').S3Client} s3
   * @property {string} s3BucketName
   * @property {URL} s3BaseURL
   *
   * @returns {Promise<ReadContext>}
   */
   export async function getReadContext() {
    const dbConnection = getRequiredEnv('RO_DATABASE_CONNECTION')
    const s3Region = getRequiredEnv('S3_REGION')
    const accessKeyId = getRequiredEnv('S3_ACCESS_KEY_ID')
    const secretAccessKey = getRequiredEnv('S3_SECRET_ACCESS_KEY')
    const s3BucketName = getRequiredEnv('S3_BUCKET_NAME')
    const s3Endpoint = process.env.S3_ENDPOINT
  
    const db = await getDbClient(dbConnection)
    const s3 = new S3Client({
      endpoint: s3Endpoint,
      forcePathStyle: !!s3Endpoint, // Force path if endpoint provided
      region: s3Region,
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
    })
  
    const s3BaseURL = s3Endpoint
      ? new URL(`${s3BucketName}/`, s3Endpoint)
      : new URL(`https://${s3BucketName}.s3.${s3Region}.amazonaws.com/`)
  
    return {
      db,
      s3,
      s3BucketName,
      s3BaseURL,
    }
  }


  /**
   * 
   * @typedef {object} UpdateContext
   * @property {import('pg').Client} db
   * 
   * @returns {Promise<UpdateContext>}
   */
  export async function getUpdateContext() {
      const dbConnection = getRequiredEnv('DATABASE_CONNECTION')
      const db = await getDbClient(dbConnection)
      return { db }
  }


    
  /**
   * @param {string} key
   * @returns {string} process.env[key]
   * @throws if env var is not set
   */
   function getRequiredEnv(key) {
    const val = process.env[key]
    if (val === undefined) {
      throw new Error('missing env var ' + key)
    }
    return val
  }
  