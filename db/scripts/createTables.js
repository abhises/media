import pkg from "pg";
const { Client } = pkg;

async function createTables() {
  const client = new Client({
    host: "localhost",
    port: 5432,
    user: "media",
    password: "media",
    database: "media",
  });

  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL");

    // List of SQL table creation commands
    const tableQueries = [
      `
     CREATE TABLE IF NOT EXISTS media_audit (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(72) NOT NULL,
        occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        actor_user_id VARCHAR(191),
        action VARCHAR(100) NOT NULL,
        before_json JSONB,
        after_json JSONB
      );
      `,
      `
     CREATE TABLE IF NOT EXISTS media (
        id SERIAL PRIMARY KEY,

        -- identities
        media_id VARCHAR(72) UNIQUE NOT NULL,
        owner_user_id VARCHAR(191) NOT NULL,
        new_owner_user_id VARCHAR(191),
        collection_id VARCHAR(72),
        actor_user_id VARCHAR(191),
        created_by_user_id VARCHAR(191),
        updated_by_user_id VARCHAR(191),

        -- enums
        media_type VARCHAR(20) CHECK (
        media_type IN ('audio', 'video', 'image', 'gallery', 'file')
        ),
        visibility VARCHAR(20) CHECK (
            visibility IN ('public', 'private', 'subscribers', 'purchasers', 'unlisted')
        ),
        status VARCHAR(50) CHECK (
            status IN ('draft', 'pending_review', 'scheduled', 'published', 'archived', 'deleted')
        ),
        -- text & meta
        title VARCHAR(255),
        description TEXT,
        media_meta JSONB,
        image_variants_json JSONB,
        file_extension VARCHAR(16),
        file_name VARCHAR(255),

          -- urls
        asset_url TEXT CHECK (asset_url ~* '^https?://'),
        poster_url TEXT CHECK (poster_url ~* '^https?://'),
        gallery_poster_url TEXT CHECK (gallery_poster_url ~* '^https?://'),

        -- numbers
        file_size_bytes BIGINT CHECK (file_size_bytes >= 0),
        duration_seconds INTEGER CHECK (duration_seconds >= 0),
        video_width INTEGER CHECK (video_width >= 0),
        video_height INTEGER CHECK (video_height >= 0),
        expected_version INTEGER DEFAULT 0 CHECK (expected_version >= 0),
        version INTEGER DEFAULT 0 CHECK (version >= 0),
        position INTEGER DEFAULT 0 CHECK (position >= 0),
        "limit" INTEGER DEFAULT 0 CHECK ("limit" >= 0 AND "limit" <= 100),
        blurred_value_px INTEGER DEFAULT 0 CHECK (blurred_value_px >= 0 AND blurred_value_px <= 40),
        trailer_blurred_value_px INTEGER DEFAULT 0 CHECK (trailer_blurred_value_px >= 0 AND trailer_blurred_value_px <= 40),

        -- booleans
        featured BOOLEAN DEFAULT FALSE,
        coming_soon BOOLEAN DEFAULT FALSE,
        pending_conversion BOOLEAN DEFAULT FALSE,
        include_tags BOOLEAN DEFAULT FALSE,
        include_coperformers BOOLEAN DEFAULT FALSE,
        placeholder_lock BOOLEAN DEFAULT FALSE,
        blurred_lock BOOLEAN DEFAULT FALSE,
        trailer_blurred_lock BOOLEAN DEFAULT FALSE,
        is_deleted BOOLEAN DEFAULT FALSE,
        soft_delete BOOLEAN DEFAULT FALSE,
        hard_delete BOOLEAN DEFAULT FALSE,
        merge BOOLEAN DEFAULT FALSE,

        -- arrays / JSON lists
        tags JSONB,
        coperformers JSONB,
        performer_ids JSONB,

        -- misc
        idempotency_key VARCHAR(191),
        cursor VARCHAR(191),
        query VARCHAR(500),

        -- dates
        publish_date TIMESTAMP,
        entry_date TIMESTAMP,
        last_updated TIMESTAMP,
        deleted_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `,
      `
       CREATE TABLE IF NOT EXISTS collections (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(72) UNIQUE NOT NULL,
        owner_user_id VARCHAR(191) NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        visibility VARCHAR(20) DEFAULT 'private',
        poster_url TEXT CHECK (poster_url LIKE 'https%'),
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
      `,
      `
       CREATE TABLE IF NOT EXISTS collection_media (
        id SERIAL PRIMARY KEY,
        collection_id VARCHAR(72) NOT NULL,
        media_id VARCHAR(72) NOT NULL,
        position INTEGER DEFAULT 0 CHECK (position >= 0),
        FOREIGN KEY (collection_id) REFERENCES collections (collection_id) ON DELETE CASCADE,
        FOREIGN KEY (media_id) REFERENCES media (media_id) ON DELETE CASCADE
      );
      `,
      `
      CREATE TABLE IF NOT EXISTS media_coperformers (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(72) NOT NULL,
        performer_id VARCHAR(191) NOT NULL,
        UNIQUE (media_id, performer_id),
        FOREIGN KEY (media_id) REFERENCES media (media_id) ON DELETE CASCADE
      );
      `,
      `CREATE TABLE IF NOT EXISTS media_tags (
        id SERIAL PRIMARY KEY,
        media_id VARCHAR(72) NOT NULL REFERENCES media(media_id) ON DELETE CASCADE,
        tag VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (media_id, tag)
      );
      `,
    ];

    // Run each query
    for (const query of tableQueries) {
      await client.query(query);
    }

    console.log("🧱 All tables created successfully!");
  } catch (err) {
    console.error("❌ Error creating tables:", err);
  } finally {
    await client.end();
    console.log("🔌 Connection closed");
  }
}

createTables();
