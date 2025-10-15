import pkg from 'pg';
const { Client } = pkg;

async function dropTables() {
  const client = new Client({
    host: 'localhost',
    port: 5432,
    user: 'media',
    password: 'media',
    database: 'media',
  });

  try {
    await client.connect();
    console.log('✅ Connected to PostgreSQL');

    // List of tables to drop (in correct dependency order)
    const tables = [
      'collection_media',
      'media_coperformers',
      'media_tags',
      'collections',
      'media_audit',
      'media'
    ];

    for (const table of tables) {
      await client.query(`DROP TABLE IF EXISTS ${table} CASCADE;`);
      console.log(`🗑️ Table dropped: ${table}`);
    }

    console.log('✅ All tables dropped successfully!');
  } catch (err) {
    console.error('❌ Error dropping tables:', err);
  } finally {
    await client.end();
    console.log('🔌 Connection closed');
  }
}

dropTables();
