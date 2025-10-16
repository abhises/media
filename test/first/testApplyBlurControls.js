import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // Connects to your DB

export default async function testApplyBlurControls() {
  console.log("Starting test for applyBlurControls...");

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
  // ⚙️ Step 1: Ensure a media item exists in DB
  // (You can reuse handleAddMediaItem or insert one manually)
  // ---------------------------------
  const existingMediaId = "5d9f1333-bd81-4e20-a3eb-5840e9a68a60"; // Replace with a real existing media_id

  // ---------------------------------
  // ⚙️ Step 2: Prepare payload for applying blur controls
  // ---------------------------------
  const payload = {
    media_id: existingMediaId,
    expectedVersion: 6, // Replace with the actual version from DB
    placeholder_lock: true,
    blurred_lock: true,
    blurred_value_px: 10,
    trailer_blurred_lock: false,
    trailer_blurred_value_px: 5,
    actorUserId: 99,
  };

  // ---------------------------------
  // ⚙️ Step 3: Run the method
  // ---------------------------------
  try {
    const result = await service.applyBlurControls(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

// Run test directly
testApplyBlurControls();
