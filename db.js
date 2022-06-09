import pg from 'pg'
import retry from 'p-retry'


export async function getDbClient(connectionString) {
    return retry(async () => {
      console.log('connecting to db at', connectionString)
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
  