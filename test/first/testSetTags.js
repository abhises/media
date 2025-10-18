import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // connects to your real DB

export default async function testSetTags() {
  console.log("Starting test for setTags...");

  // ✅ Real DB instance
  const db = new DB();

  // ✅ Logger and dependencies
  const log = { info: (...args) => console.log("LOG:", ...args) };
  const indexer = { upsert: async (id) => console.log("Indexer upsert:", id) };
  const clock = { now: () => new Date() };
  const uuid = { v4: () => "test-uuid-" + Date.now() };

  // ✅ Create MediaService instance
  const service = new MediaService(log, indexer, clock, uuid);
  service.db = db;
  service.log = log;

  // ---------------------------------
  // ⚙️ Step 1: Ensure a media record exists in DB
  // (You can check the media table to confirm)
  // ---------------------------------
  const existingMediaId = "test123"; // Replace with your real media_id

  // ---------------------------------
  // ⚙️ Step 2: Prepare payload
  // ---------------------------------
  const payload = {
    media_id: existingMediaId,
    expectedVersion: 3, // replace with the actual version in DB
    tags: ["action", "drama", "thriller"], // example tags to apply
    actorUserId: 42,
  };

  // ---------------------------------
  // ⚙️ Step 3: Run the method
  // ---------------------------------
  try {
    const result = await service.setTags(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  } finally {
    // ✅ Always close DB connection if your DB class exposes close()
    if (typeof db.close === "function") await db.close();
  }
}

// Run test directly
testSetTags();
