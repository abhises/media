import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // your DB connection or mock DB

export default async function testSetComingSoon() {
  console.log("Starting test for setComingSoon...");

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
  // Replace with a real ID if testing against an actual DB
  const existingMediaId = "5d9f1333-bd81-4e20-a3eb-5840e9a68a60";

  // ✅ Step 2: Prepare payload
  const payload = {
    media_id: existingMediaId,
    expectedVersion: 3, // replace with correct version if needed
    coming_soon: true, // toggle true/false to test both cases
    actorUserId: 42,
  };

  // ✅ Optional: mock _simpleFieldUpdate if you don’t want DB dependency
//   service._simpleFieldUpdate = async ({ media_id, fields }) => {
//     console.log(`Mock _simpleFieldUpdate called for ${media_id}`);
//     console.log("Updated fields:", fields);
//     return { version: 4, ...fields }; // simulate DB update response
//   };

  // ✅ Step 3: Run the method
  try {
    const result = await service.setComingSoon(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    console.error("❌ Test failed:", err);
  }
}

// Run test directly
testSetComingSoon();
