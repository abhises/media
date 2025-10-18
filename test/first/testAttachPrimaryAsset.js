import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // Connects to your DB

export default async function testAttachPrimaryAsset() {
  console.log("Starting test for attachPrimaryAsset...");

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
  const existingMediaId = "test123"; // Replace with a real existing media_id

  // ---------------------------------
  // ⚙️ Step 2: Prepare payload for attaching primary asset
  // ---------------------------------
  const payload = {
    media_id: existingMediaId,
    expectedVersion: 6, // Replace with the actual version from DB
    asset_url: "http://example.com/testing/manytimes/video.mp4",
    file_extension: "mp4",
    file_name: "video.mp4",
    file_size_bytes: 10485760,
    duration_seconds: 120,
    video_width: 1920,
    video_height: 1080,
    pending_conversion: false,
    actorUserId: 99,
  };

  // ---------------------------------
  // ⚙️ Step 3: Run the method
  // ---------------------------------
  try {
    const result = await service.attachPrimaryAsset(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

// Run test directly
testAttachPrimaryAsset();
