import test from 'ava'
import pg from 'pg'
import { PostgreSqlContainer } from 'testcontainers'

import { updateBackupUrls } from '../url-put.js'
import { BackfillState } from '../state.js'
import { initDBSchema } from './db-helpers.js'


// let dbContainer
// let dbClient


// test.before(async (t) => {
//     t.timeout(60 * 1000, 'wait for db container to start')

//     dbContainer = await new PostgreSqlContainer().start()
//     dbClient = new pg.Client({
//         host: dbContainer.getHost(),
//         port: dbContainer.getPort(),
//         database: dbContainer.getDatabase(),
//         user: dbContainer.getUsername(),
//         password: dbContainer.getPassword(),
//     })
//     await dbClient.connect()
// })

// test.beforeEach(async () => {
//     await initDBSchema(dbClient)
// })

// test.after(async (t) => {
//     await dbClient.end()
//     await dbContainer.stop()
// })



test('it should fail if there are no candidates in the state db', async (t) => {
    const stateDB = await BackfillState.open({ filename: '/tmp/fixme-tmpfile.foo' })

    t.throws(async () => {
        updateBackupUrls({ stateDB })
    })
})