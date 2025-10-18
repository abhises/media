import MediaService from "../../service/MediaHandler.js";
import DB from "../../utils/DB.js"; // connects to your real DB
import { NotFoundError, ConflictError } from "../../utils//Error_handler.js";

export default async function testSetCustomMeta() {
  console.log("Starting test for setCustomMeta...");

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
  // ⚙️ Step 1: Ensure a media record exists in your DB
  // ---------------------------------
  const existingMediaId = "f4a881fc-440c-4887-a811-bf6c6a9ed70e"; // Replace with actual media_id
  const expectedVersion = 1; // must match current version in DB

  // ---------------------------------
  // ⚙️ Step 2: Prepare payload
  // ---------------------------------
  const payload = {
    media_id: existingMediaId,
    media_meta: { customField: "newValue testing and updating" }, // meta to set
    merge: true,                             // merge with existing meta
    expectedVersion,                         // must match DB version
    actorUserId: 42,                         // user performing the action
  };

  // ---------------------------------
  // ⚙️ Step 3: Run the method
  // ---------------------------------
  try {
    const result = await service.setCustomMeta(payload);
    console.log("✅ Test finished successfully:");
    console.log(result);
  } catch (err) {
    if (err instanceof NotFoundError) {
      console.error("❌ Test failed: Media not found");
    } else if (err instanceof ConflictError) {
      console.error("❌ Test failed: Version conflict");
    } else {
      console.error("❌ Test failed:", err);
    }
  } finally {
    // ✅ Always close DB connection if available
    if (typeof db.close === "function") await db.close();
  }
}

// Run directly
testSetCustomMeta();
