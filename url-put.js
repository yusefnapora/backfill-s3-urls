import { BackfillState } from './state'
import { getUpdateContext } from './context'

/**
 * 
 * @param {{ stateDB: string, batchSize: number, interval: number }} options 
 */
export async function updateBackupUrls({ stateDB, batchSize, interval }) {
    const state = await BackfillState.open(stateDB)
    const { db } = await getUpdateContext()

    const { total, checkedS3, backfilled } = await state.getCandidateCounts()

    if (total === 0) {
        throw new Error('state db contains no candidates to update. run get-urls first and set the --stateDB flag to the generated .db file')
    }

    if (total !== checkedS3) {
        console.warn(`⚠️ ${total - checkedS3} entries have not yet been checked on s3`)
    }

    let remaining = total - backfilled
    while (remaining > 0) {
        console.log(`backfilling ${batchSize} / ${total} uploads. remaining: ${remaining}`)

        const candidates = await state.getBackfillableCandidates({ limit: batchSize })
        for (const c of candidates) {
            const urls = await state.getDiscoveredUrls(c.id)
            await updateUrlsForUpload(db, c.id, urls)
            await state.markBackfilled(c.id)
        }

        remaining -= candidates.length

        console.log(`batch complete. delaying ${interval} sec`)
        await new Promise((resolve) => setTimeout(resolve, interval * 1000))
    }

    console.log('backfill complete')
    await state.close()
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