import Database from 'better-sqlite3'


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
    constructor(filename) {
        if (typeof filename !== 'string') {
            throw new Error('must provide string filename.')
        }

        this.db = new Database(filename)
        this._initDB()
    }

    _initDB() {
        // candidates are uploads that we think may be missing backup_urls.
        this.db.exec(
            `CREATE TABLE IF NOT EXISTS candidate (
                id INTEGER PRIMARY KEY, -- same as upload.id from nft.storage database
                source_cid TEXT NOT NULL,
                user_id TEXT NOT NULL,
                checked_s3_at TEXT DEFAULT NULL, -- datetime when this candidate was checked on s3. null if not yet checked.
                backfilled_at TEXT DEFAULT NULL -- datetime when this candidate was corrected in the db. null if not yet backfilled
            )`
        )

        // all discovered backup urls for each candidate id
        this.db.exec(
            `CREATE TABLE IF NOT EXISTS url (
                candidate_id INTEGER,
                url TEXT,
                FOREIGN KEY(candidate_id) REFERENCES candidate(id)
            )`
        ) 
    }

    close() {
        return this.db.close()
    }

    /**
     * Add a candidate entry to the tracking db.
     * @param {CandidateUpload[]} candidate 
     */
    addCandidates(candidates) {
        const batchSize = 300
        const queryBase = 'INSERT INTO candidate (id, source_cid, user_id) VALUES '

        for (let i = 0; i < candidates.length; i += batchSize) {
            const batch = candidates.slice(i, i+batchSize)

            const placeholders = batch.map(() => `(?, ?, ?)`).join(', ')
            const stmt = this.db.prepare(queryBase + placeholders)
            stmt.run(...values)
        }
    }

    /**
     * Update a candidate entry with URLs discovered from s3.
     * 
     * @param {number|string} candidateId 
     * @param {URL[]} urls 
     * @param {Date} [timestamp] 
     */
    addDiscoveredUrls(candidateId, urls, timestamp = undefined) {
        if (!timestamp) {
            timestamp = new Date()
        }
        const insertStmt = this.db.prepare(`INSERT INTO url (candidate_id, url) VALUES (?, ?)`)
        for (const url of urls) {
            insertStmt.run(candidateId, url.toString())
        }

        const updateTimestampStmt = this.db.prepare(
            `UPDATE candidate
             SET checked_s3_at = ?
             WHERE id = ?
            `
        )
        updateTimestampStmt.run(timestamp.toISOString(), candidateId)
    }

    /**
     * Mark a candidate entry as backfilled (updated in prod db)
     * @param {number|string} candidateId 
     * @param {Date} [timestamp] 
     */
    markBackfilled(candidateId, timestamp = undefined) {
        if (!timestamp) {
            timestamp = new Date()
        }

        const stmt = this.db.prepare(
            `UPDATE candidate
             SET backfilled_at = ?
             WHERE id = ?`,
        )

        stmt.run(timestamp.toISOString(), candidateId)
    }

    /**
     * Returns an array of {@link CandidateUpload}s that haven't yet
     * been checked on s3.
     * 
     * @returns {CandidateUpload[]}
     */
    getUncheckedCandidates() {
        const stmt = this.db.prepare(
            `SELECT id, source_cid, user_id 
             FROM candidate
             WHERE checked_s3_at IS NULL`
        )
        return stmt.all()
    }

    getBackfillableCandidates({ limit = 100} = {}) {
        const stmt = this.db.prepare(
            `SELECT id, source_cid, user_id, checked_s3_at
             FROM candidate
             WHERE checked_s3_at IS NOT NULL
             AND backfilled_at IS NULL
             LIMIT ?`
        )
        return stmt.all(limit)
    }

    /**
     * Returns all discovered backup urls for the given candidate id.
     * 
     * @param {number|string} candidateId 
     * @returns {string[]}
     */
    getDiscoveredUrls(candidateId) {
        const stmt = this.db.prepare(
            `SELECT url
             FROM url
             WHERE candidate_id = ?`
        )
        const rows = stmt.all(candidateId)
        return rows.map((row) => row.url)
    }

    /**
     * @returns {Promise<{ total: number, checkedS3: number, backfilled: number }>} the number of candidate entries
     */
    getCandidateCounts() {
        const { total } = this.db.prepare(`SELECT COUNT(id) as total from candidate`).get()
        const { checkedS3 } = this.db.prepare(`SELECT COUNT(id) as checkedS3 FROM candidate WHERE checked_s3_at IS NOT NULL`).get()
        const { backfilled } = this.db.prepare(`SELECT COUNT(id) as backfilled FROM candidate WHERE backfilled_at IS NOT NULL`).get()

        return { total, checkedS3, backfilled }
    }
}