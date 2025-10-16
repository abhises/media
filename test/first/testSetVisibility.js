import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // your DB connection or mock DB

export default async function testSetVisibility() {
  console.log("Starting test for setVisibility...");

  // ✅ Mock dependencies
  const db = new DB();
  const log = { info: (...args) => console.log("LOG:", ...args) };
  const indexer = { upsert: async (id) => console.log("Indexer upsert:", id) };
  const clock = { now: () => new Date() };
  const uuid = { v4: () => "test-uuid-" + Date.now() };

  // ✅ Create MediaService instance
  const service = new MediaService(log, indexer, clock, uuid);
  service.db = db;
  service.log = log;

  // ✅ Step 1: Ensure a media item exists in DB
  // Replace this with a real ID if you're testing against an actual DB
  const existingMediaId = "5d9f1333-bd81-4e20-a3eb-5840e9a68a60";

  // ✅ Step 2: Prepare payload
  const payload = {
    media_id: existingMediaId,
    expectedVersion: 3, // Replace with the actual version in your DB
    visibility: "subscribers", // or "public"
    actorUserId: 42,
  };

  // ✅ Step 3: Run the method
  try {
    const result = await service.setVisibility(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

// Run test directly
testSetVisibility();
