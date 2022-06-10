import test from 'ava'
import pg from 'pg'
import { PostgreSqlContainer } from 'testcontainers'

import { updateBackupUrls } from '../url-put.js'
import { BackfillState } from '../state.js'
import { initDBSchema } from './db-helpers.js'


let dbContainer
let connectionString

test.before(async (t) => {
    t.timeout(60 * 1000, 'wait for db container to start')

    console.log('starting database container....')
    dbContainer = await new PostgreSqlContainer().start()
    const host = dbContainer.getHost()
    const port = dbContainer.getPort()
    const database = dbContainer.getDatabase()
    const user = dbContainer.getUsername()
    const password = dbContainer.getPassword()
    connectionString = `postgres://${user}:${password}@${host}:${port}/${database}`
    process.env.DATABASE_CONNECTION = connectionString

    console.log('database container running')
})

test.beforeEach(async () => {
    const dbClient = new pg.Client({connectionString})
    await dbClient.connect()
    await initDBSchema(dbClient)
    await dbClient.end()
})

test.after(async (t) => {
    await dbContainer.stop()
})



test('it should fail if there are no candidates in the state db', async (t) => {
    const stateDB = new BackfillState(':memory:')
    await t.throwsAsync(updateBackupUrls({ stateDB }))
})