import throttledQueue from 'throttled-queue'

import { BackfillState } from './state.js'
import { getUpdateContext } from './context.js'

/**
 * 
 * @param {{ stateDB: string, requestsPerSecond: number }} options 
 */
export async function updateBackupUrls({ stateDB, requestsPerSecond }) {
    const state = new BackfillState.open({ stateDB })
    const { db } = await getUpdateContext()

    const throttle = throttledQueue(requestsPerSecond, 1000)

    const { total, checkedS3, backfilled } = state.getCandidateCounts()

    if (total === 0) {
        throw new Error('state db contains no candidates to update. run get-urls first and set the --stateDB flag to the generated .db file')
    }

    if (total !== checkedS3) {
        console.warn(`⚠️ ${total - checkedS3} entries have not yet been checked on s3`)
    }

    let remaining = total - backfilled
    while (remaining > 0) {
        console.log(`backfilling ${batchSize} / ${total} uploads. remaining: ${remaining}`)

        const candidates = state.getBackfillableCandidates({ limit: batchSize })
        for (const c of candidates) {
            const urls = state.getDiscoveredUrls(c.id)
            await throttle(() => updateUrlsForUpload(db, c.id, urls))
            state.markBackfilled(c.id)
        }

        remaining -= candidates.length
    }

    console.log('backfill complete')
    state.close()
}

/**
 * 
 * @param {import('pg').Client} db 
 * @param {string|number} uploadId
 * @param {string[]} urls
 */
async function updateUrlsForUpload(db, uploadId, urls) {
    await db.query(
        `UPDATE upload
         SET backup_urls = SELECT ARRAY(SELECT json_array_elements_text( $1 ))
         WHERE upload.id = $2`,
        urls,
        uploadId
    )
}