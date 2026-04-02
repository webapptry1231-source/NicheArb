import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

async function runMigration() {
  const pg = new Pool({
    connectionString: process.env.DATABASE_URL,
  });

  try {
    console.log('Running migration...');

    await pg.query(`
      ALTER TABLE arb_routes 
      ALTER COLUMN cooldown_until TYPE BIGINT;

      ALTER TABLE active_pools 
      ALTER COLUMN last_scanned_block TYPE BIGINT,
      ALTER COLUMN last_tvl_update TYPE BIGINT;
    `);

    console.log('Migration completed successfully ✅');
  } catch (err) {
    console.error('Migration failed ❌', err);
  } finally {
    await pg.end();
    process.exit(0);
  }
}

runMigration();
