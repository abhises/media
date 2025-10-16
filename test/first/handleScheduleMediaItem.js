import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // Connects to your DB

export default async function handleScheduleMediaItem() {
  console.log("Starting test for handleScheduleMediaItem...");

  // Real or mock DB instance
  const db = new DB();

  // Simple logger
  const log = { info: (...args) => console.log("LOG:", ...args) };
  const indexer = { upsert: async (id) => console.log("Indexer upsert:", id) };
  const clock = { now: () => new Date() };
  const uuid = { v4: () => "test-uuid-" + Date.now() };

  // Create MediaService instance
  const service = new MediaService(log, indexer, clock, uuid);
  service.db = db;
  service.log = log;

  // ---------------------------------
  // ⚙️ Step 1: Make sure a media item exists in DB
  // (You can reuse handleAddMediaItem or insert one manually)
  // ---------------------------------
  const existingMediaId = "c8239aea-5a4a-4464-9f73-446d45e2f114"; // Replace with a real existing media_id

  // ---------------------------------
  // ⚙️ Step 2: Prepare payload for scheduling
  // ---------------------------------
  const payload = {
    media_id: existingMediaId,
    expectedVersion: 6, // Replace with actual version from DB
    publish_date: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(), // schedule for tomorrow
    actorUserId: 99,
  };

  // ---------------------------------
  // ⚙️ Step 3: Run the method
  // ---------------------------------
  try {
    const result = await service.handleScheduleMediaItem(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

// Run test directly
handleScheduleMediaItem();
