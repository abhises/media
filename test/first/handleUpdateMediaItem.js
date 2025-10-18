import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // Make sure this connects to your real DB

export default async function handleUpdateMediaItem() {
  console.log("Starting test for handleUpdateMediaItem...");

  // Real DB instance
  const db = new DB();

  const log = { info: (...args) => console.log("LOG:", ...args) };
  const indexer = { upsert: async () => {}, delete: async () => {} };
  const clock = { now: () => new Date() };
  const uuid = { v4: () => "test-uuid-" + Date.now() };

  // Create instance of your service
  const service = new MediaService(log, indexer, clock, uuid);
  service.db = db; // inject real DB
  service.log = log; // optional

  // Test payload
  const payload = {
    media_id: "test123", // existing media ID to update
    tags: ["tag1"],
    coperformers: [1, 2],
    asset_url: "http://asset.url",
    poster_url: "http://poster.url",
    placeholder_lock: true,
    blurred_lock: true,
    blurred_value_px: 10,
    trailer_blurred_lock: true,
    trailer_blurred_value_px: 5,
    owner_user_id: "42",
    media_type: "video", // required
    visibility: "public",
    title: "Test Media",
    description: "Media inserted during test",
    expectedVersion:6,
    actorUserId: 99
    

  };


  try {
    // Call the real method that inserts into DB
    const result = await service.handleUpdateMediaItem({ ...payload});

    console.log("✅ Test finished, media updated:", result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

// Run the test
handleUpdateMediaItem();
