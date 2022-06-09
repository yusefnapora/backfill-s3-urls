import pg from 'pg'
import retry from 'p-retry'


export async function getDbClient(connectionString) {
    const redacted = connectionString
      .replace(/^(\w+):\/\/(\w+):[^@]+@/, '$1://$2:xxxx@')
      .replace(/password=.*([&?])?/, 'password=xxxx$1')

    console.log('connecting to db at', redacted)

    return retry(async () => {
      try {
      const c = new pg.Client({ connectionString })
      await c.connect()
      return c
      } catch (err) {
        console.error('connection attempt failed: ' + err)
        throw err
      }
    })
  }
  