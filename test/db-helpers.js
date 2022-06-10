import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const schemaDir = path.join(__dirname, 'schema')

/**
 * Applies nft.storage database schema to the connected DB.
 * @param {import('pg').Client} dbClient 
 */
export async function initDBSchema(dbClient) {
    const sqlStatements = [
        loadSql('config.sql'),
        loadSql('reset.sql'),
        loadSql('tables.sql'),
        loadSql('cargo.testing.sql'),
        loadSql('functions.sql'),
    ]

    for (const stmt of sqlStatements) {
        await dbClient.query(stmt)
    }
}

export async function getUploadId(dbClient, source_cid, user_id) {
    const { id } = await dbClient.query(
        'SELECT id FROM upload WHERE source_cid = $1 AND user_id = $2 AND deleted_at IS NULL'
    )
    return id
}

export async function addUpload(dbClient, {
    content_cid,
    source_cid,
    user_id,
    name = 'upload',
    files = [],
    origins = [],
    meta = {},
    backup_urls = [],
    type = 'Blob',
    mime_type = '',
    dag_size = 0,
    updated_at = new Date(),
    inserted_at = new Date(),
}) {
    await dbClient.rpc('create_upload', { data: {
        content_cid,
        source_cid,
        user_id,
        name,
        files,
        origins,
        meta,
        backup_urls,
        type,
        mime_type,
        dag_size,
        updated_at,
        inserted_at,
    }})
}

/**
 * @param {string} file
 */
 function loadSql(file) {
    return fs.readFileSync(path.join(schemaDir, file), 'utf8')
  }