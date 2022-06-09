import { PromisedDatabase } from 'promised-sqlite3'


/**
 * @typedef {object} CandidateUpload
 * @property {number|string} id
 * @property {string} source_cid
 * @property {number|string} user_id
 * @property {Date} [checked_s3_at]
 * @property {Date} [backfilled_at]
 * 
 */

export class BackfillState {
    constructor(stateDB, filename) {
        this.db = stateDB
        this.filename = filename
    }

    /**
     * 
     * @param {{ filename?: string }} options 
     * @returns 
     */
    static async open({ filename } = {}) {
        if (!filename) {
            filename = `./backfill-${new Date().toISOString()}.db`
        }
        const db = new PromisedDatabase()
        await db.open(filename)
        await this._initDB(db)
        return new BackfillState(db, filename)
    }

    static async _initDB(db) {

        // candidates are uploads that we think may be missing backup_urls.
        await db.run(
            `CREATE TABLE IF NOT EXISTS candidate (
                id INTEGER PRIMARY KEY, -- same as upload.id from nft.storage database
                source_cid TEXT NOT NULL,
                user_id TEXT NOT NULL,
                checked_s3_at TEXT DEFAULT NULL, -- datetime when this candidate was checked on s3. null if not yet checked.
                backfilled_at TEXT DEFAULT NULL -- datetime when this candidate was corrected in the db. null if not yet backfilled
            )`
        )

        // all discovered backup urls for each candidate id
        await db.run(
            `CREATE TABLE IF NOT EXISTS url (
                candidate_id INTEGER,
                url TEXT,
                FOREIGN KEY(candidate_id) REFERENCES candidates(id)
            )`
        ) 
    }

    async close() {
        return this.db.close()
    }

    /**
     * Add a candidate entry to the tracking db.
     * @param {CandidateUpload[]} candidate 
     */
    async addCandidates(candidates) {
        const batchSize = 300
        const queryBase = 'INSERT INTO candidate (id, source_cid, user_id) VALUES '

        for (let i = 0; i < candidates.length; i += batchSize) {
            const batch = candidates.slice(i, i+batchSize)

            const placeholders = batch.map(() => `(?, ?, ?)`).join(', ')
            const values = batch.flatMap(({ id, source_cid, user_id }) => [id, source_cid, user_id])
            await this.db.run(queryBase + placeholders, ...values)
        }
    }

    /**
     * Update a candidate entry with URLs discovered from s3.
     * 
     * @param {number|string} candidateId 
     * @param {string[]} urls 
     * @param {Date} [timestamp] 
     */
    async addDiscoveredUrls(candidateId, urls, timestamp = undefined) {
        if (!timestamp) {
            timestamp = new Date()
        }
        for (const url of urls) {
            await this.db.run(
                `INSERT INTO url (candidate_id, url) VALUES (?, ?)`,
                candidateId,
                url,
            )
        }
        await this.db.run(
            `UPDATE candidate
             SET checked_s3_at = ?
             WHERE id = ?
            `,
            timestamp.toISOString(),
            candidateId
        )
    }

    /**
     * Mark a candidate entry as backfilled (updated in prod db)
     * @param {number|string} candidateId 
     * @param {Date} [timestamp] 
     */
    async markBackfilled(candidateId, timestamp = undefined) {
        if (!timestamp) {
            timestamp = new Date()
        }

        await this.db.run(
            `UPDATE candidate
             SET backfilled_at = ?
             WHERE id = ?`,
             timestamp.toISOString(),
             candidateId,
        )
    }

    /**
     * Returns an array of {@link CandidateUpload}s that haven't yet
     * been checked on s3.
     * 
     * @returns {Promise<CandidateUpload[]>}
     */
    async getUncheckedCandidates() {
        const rows = await this.db.all(
            `SELECT id, source_cid, user_id 
             FROM candidate
             WHERE checked_s3_at IS NULL`)
        return rows.map(({id, source_cid, user_id}) => ({
            id,
            source_cid,
            user_id,
        }))
    }

    async getBackfillableCandidates({ limit = 100} = {}) {
        const rows = await this.db.all(
            `SELECT id, source_cid, user_id, checked_s3_at
             FROM candidate
             WHERE checked_s3_at IS NOT NULL
             AND backfilled_at IS NULL
             LIMIT ?`,
             limit
        )
        return rows.map(({id, source_cid, user_id, checked_s3_at}) => ({
            id,
            source_cid,
            user_id,
            checked_s3_at,
        }))
    }

    /**
     * Returns all discovered backup urls for the given candidate id.
     * 
     * @param {number|string} candidateId 
     * @returns {Promise<string[]>}
     */
    async getDiscoveredUrls(candidateId) {
        const rows = await this.db.all(
            `SELECT url
             FROM url
             WHERE candidate_id = ?`,
             candidateId
        )
        return rows.map((row) => row[0])
    }

    /**
     * @returns {Promise<{ total: number, checkedS3: number, backfilled: number }>} the number of candidate entries
     */
    async getCandidateCounts() {
        const { total } = await this.db.get(`SELECT COUNT(id) as total from candidate`)
        const { checkedS3 } = await this.db.get(`SELECT COUNT(id) as checkedS3 FROM candidate WHERE checked_s3_at IS NOT NULL`)
        const { backfilled } = await this.db.get(`SELECT COUNT(id) as backfilled FROM candidate WHERE backfilled_at IS NOT NULL`)

        return { total, checkedS3, backfilled }
    }
}