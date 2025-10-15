import DB from "../utils/DB.js";

const db = new DB();

export default async function dbInserting() {
    try {
    const newMedia = await db.insert("default", "media", {
      media_id: "test123",
      owner_user_id: 1,
      created_by_user_id: 1,
      updated_by_user_id: 1,
      media_type: "image",
      status: "draft",
      visibility: "public",
      title: "Test Media",
      description: "Just a test insert",
      asset_url: "https://example.com/test.jpg",
      file_extension: "jpg",
      file_name: "test.jpg",
      file_size_bytes: 1024,
      entry_date: new Date(),
    });

    console.log("✅ Inserted media:", newMedia);
  } catch (err) {
    console.error("❌ Insert failed:", err);
  } finally {
    await db.closeAll();
  }
}
dbInserting()