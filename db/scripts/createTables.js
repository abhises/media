import pkg from 'pg';
const { Client } = pkg;

async function createTables() {
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

    // List of SQL table creation commands
    const tableQueries = [
      `
      CREATE TABLE IF NOT EXISTS media_audit (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(100) NOT NULL,
        occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        actor_user_id INTEGER,
        action VARCHAR(100) NOT NULL,
        before_json JSONB,
        after_json JSONB
      );
      `, `
      CREATE TABLE IF NOT EXISTS media (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(100) UNIQUE NOT NULL,
        owner_user_id INTEGER,
        created_by_user_id INTEGER,
        updated_by_user_id INTEGER,
        media_type VARCHAR(50),
        status VARCHAR(50),
        visibility VARCHAR(50),
        title TEXT,
        description TEXT,
        featured BOOLEAN DEFAULT FALSE,
        coming_soon BOOLEAN DEFAULT FALSE,
        asset_url TEXT,
        file_extension VARCHAR(20),
        file_name TEXT,
        file_size_bytes BIGINT,
        duration_seconds INTEGER,
        video_width INTEGER,
        video_height INTEGER,
        poster_url TEXT,
        pending_conversion BOOLEAN DEFAULT FALSE,
        image_variants_json JSONB,
        gallery_poster_url TEXT,
        entry_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        publish_date TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        version INTEGER DEFAULT 1,
        is_deleted BOOLEAN DEFAULT FALSE,
        deleted_at TIMESTAMP,
        media_meta JSONB,
        placeholder_lock BOOLEAN DEFAULT FALSE,
        blurred_lock BOOLEAN DEFAULT FALSE,
        blurred_value_px INTEGER,
        trailer_blurred_lock BOOLEAN DEFAULT FALSE,
        trailer_blurred_value_px INTEGER
      );
    `,`
      CREATE TABLE IF NOT EXISTS collections (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(100) UNIQUE NOT NULL,
        owner_user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        visibility VARCHAR(50) DEFAULT 'private',
        poster_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      `,
      `
      CREATE TABLE IF NOT EXISTS collection_media (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(100) NOT NULL,
        media_id VARCHAR(100) NOT NULL,
        position INTEGER DEFAULT 0,
        FOREIGN KEY (collection_id) REFERENCES collections (collection_id) ON DELETE CASCADE,
        FOREIGN KEY (media_id) REFERENCES media (media_id) ON DELETE CASCADE
      );
      `, `
      CREATE TABLE IF NOT EXISTS media_coperformers (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(100) NOT NULL,
        performer_id INTEGER NOT NULL,
        UNIQUE (media_id, performer_id),
        FOREIGN KEY (media_id) REFERENCES media (media_id) ON DELETE CASCADE
      );
      `,
      `
      CREATE TABLE IF NOT EXISTS media_tags (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(100) NOT NULL,
        tag VARCHAR(100) NOT NULL,
        UNIQUE (media_id, tag),
        FOREIGN KEY (media_id) REFERENCES media (media_id) ON DELETE CASCADE
      );
      `
     
    ];

    // Run each query
    for (const query of tableQueries) {
      await client.query(query);
    }

    console.log('🧱 All tables created successfully!');
  } catch (err) {
    console.error('❌ Error creating tables:', err);
  } finally {
    await client.end();
    console.log('🔌 Connection closed');
  }
}

createTables();
