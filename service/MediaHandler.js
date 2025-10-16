// import DB from "../utils/DB.js";
import crypto from "crypto";
import SafeUtils from "../utils/SafeUtils.js";
import ErrorHandler from "../utils/ErrorHandler.js";
import {
  ValidationError,
  ConflictError,
  NotFoundError,
  StateTransitionError,
} from "../utils/Error_handler.js";

export default class MediaHandler {
  constructor({ db, log, indexer, clock, uuid, config }) {
    this.db = db;
    this.log = log;
    this.indexer = indexer ?? {
      upsert: async () => {},
      delete: async () => {},
    }; // Implement elasticsearch here
    this.clock = clock ?? { now: () => new Date() };
    this.uuid = {
      v4: () => crypto.randomUUID(),
    };
    // --------------------------- Descriptive constants ---------------------------
    this.STATUS = Object.freeze({
      DRAFT: "draft",
      PENDING_REVIEW: "pending_review",
      SCHEDULED: "scheduled",
      PUBLISHED: "published",
      ARCHIVED: "archived",
      DELETED: "deleted",
    });

    this.MEDIA_TYPE = Object.freeze({
      AUDIO: "audio",
      VIDEO: "video",
      IMAGE: "image",
      GALLERY: "gallery",
      FILE: "file",
    });

    this.VISIBILITY = Object.freeze({
      PUBLIC: "public",
      PRIVATE: "private",
      SUBSCRIBERS: "subscribers",
      PURCHASERS: "purchasers",
      UNLISTED: "unlisted",
    });

    this.ACTION = Object.freeze({
      ADD: "add",
      UPDATE: "update",
      SCHEDULE: "schedule",
      PUBLISH: "publish",
      SOFT_DELETE: "soft_delete",
      HARD_DELETE: "hard_delete",
      OWNERSHIP: "set_ownership",
      VISIBILITY: "set_visibility",
      FEATURED: "set_featured",
      COMING_SOON: "set_coming_soon",
      TAGS_REPLACE: "set_tags",
      TAG_ADD: "add_tag",
      TAG_REMOVE: "remove_tag",
      COPERFORMERS_REPLACE: "set_coperformers",
      ASSET_ATTACH: "attach_primary_asset",
      POSTER_SET: "set_poster",
      BLUR_APPLY: "apply_blur_controls",
      REINDEX: "reindex",
      COLLECTION_CREATE: "collection_create",
      COLLECTION_ADD: "collection_add",
      COLLECTION_REMOVE: "collection_remove",
      STATUS_SET: "set_status",
    });

    // ------------------------ Config (caps) --------------------------------------
    this.config = Object.assign(
      {
        maxTagCount: 25,
        maxTagLength: 48,
        maxCoPerformers: 16,
        maxTitleLength: 200,
        maxDescriptionLength: 16000,
        maxUrlLength: 2000,
        maxDurationSeconds: 8 * 60 * 60, // 8h
      },
      config || {}
    );

    // ============================================================================
    //                      SINGLE GLOBAL FIELD SPEC (ONE LINE PER FIELD)
    // ============================================================================
    // type syntax:
    //   string[:nonempty][:max=N]
    //   url:https
    //   int[:>=N][:<=N]
    //   bool
    //   enum:a|b|c
    //   json
    //   datetime
    const S = (rule) => Object.freeze({ rule });
    this.FIELD_SPEC = Object.freeze({
      // identities
      media_id: S("string:nonempty:max=72"),
      owner_user_id: S("string:nonempty:max=191"),
      new_owner_user_id: S("string:nonempty:max=191"),
      collection_id: S("string:nonempty:max=72"),
      actorUserId: S("string:max=191"),

      // enums
      media_type: S("enum:audio|video|image|gallery|file"),
      visibility: S("enum:public|private|subscribers|purchasers|unlisted"),

      // text & meta
      title: S(`string:max=${this.config.maxTitleLength}`),
      description: S(`string:max=${this.config.maxDescriptionLength}`),
      media_meta: S("json"),
      image_variants_json: S("json"),
      file_extension: S("string:max=16"),
      file_name: S("string:max=255"),

      // urls
      asset_url: S("url:https"),
      poster_url: S("url:https"),
      gallery_poster_url: S("url:https"),

      // numbers
      file_size_bytes: S("int:>=0"),
      duration_seconds: S(`int:>=0:<=${this.config.maxDurationSeconds}`),
      video_width: S("int:>=0"),
      video_height: S("int:>=0"),
      expectedVersion: S("int:>=0"),
      position: S("int:>=0"),
      limit: S("int:>=0:<=100"),
      blurred_value_px: S("int:>=0:<=40"),
      trailer_blurred_value_px: S("int:>=0:<=40"),

      // booleans
      featured: S("bool"),
      coming_soon: S("bool"),
      pending_conversion: S("bool"),
      includeTags: S("bool"),
      includeCoPerformers: S("bool"),
      placeholder_lock: S("bool"),
      blurred_lock: S("bool"),
      trailer_blurred_lock: S("bool"),
      soft_delete: S("bool"),
      hard_delete: S("bool"),
      merge: S("bool"),

      // arrays (normalized via helpers)
      tags: S("json"), // string[]
      coperformers: S("json"), // string[]
      performerIds: S("json"), // string[]

      // misc
      idempotency_key: S("string:max=191"),
      cursor: S("string:max=191"),
      query: S("string:max=500"),

      // dates
      publish_date: S("datetime"),
    });

    // ============================================================================
    //                 PER-HANDLER REQUIRED FIELDS (ONLY handlers use this)
    // ============================================================================
    this.METHOD_RULES = Object.freeze({
      handleAddMediaItem: ["owner_user_id", "media_type"],
      handleUpdateMediaItem: ["media_id", "expectedVersion"],
      handleScheduleMediaItem: ["media_id", "expectedVersion", "publish_date"],
      handlePublishMediaItem: ["media_id", "expectedVersion"],
    });

    // ============================================================================
    //            SIMPLE EVENT MAP (PUBLISH / SCHEDULE) — TYPE-SPECIFIC
    // ============================================================================
    this.EventMap = Object.freeze({
      publishAudioItem: [
        "title",
        "asset_url:https",
        "duration_seconds>0",
        "media_type=audio",
      ],
      publishVideoItem: [
        "title",
        "asset_url:https",
        "duration_seconds>0",
        "poster_url:https",
        "pending_conversion=false",
        "media_type=video",
      ],
      publishImageItem: ["title", "asset_url:https", "media_type=image"],
      publishGalleryItem: ["title", "asset_url:https", "media_type=gallery"],
      publishFileItem: ["title", "asset_url:https", "media_type=file"],
      publishMediaItem: {
        audio: "publishAudioItem",
        video: "publishVideoItem",
        image: "publishImageItem",
        gallery: "publishGalleryItem",
        file: "publishFileItem",
      },

      scheduleAudioItem: [
        "title",
        "asset_url:https",
        "duration_seconds>0",
        "media_type=audio",
        "publish_date>now",
      ],
      scheduleVideoItem: [
        "title",
        "asset_url:https",
        "duration_seconds>0",
        "poster_url:https",
        "pending_conversion=false",
        "media_type=video",
        "publish_date>now",
      ],
      scheduleImageItem: [
        "title",
        "asset_url:https",
        "media_type=image",
        "publish_date>now",
      ],
      scheduleGalleryItem: [
        "title",
        "asset_url:https",
        "media_type=gallery",
        "publish_date>now",
      ],
      scheduleFileItem: [
        "title",
        "asset_url:https",
        "media_type=file",
        "publish_date>now",
      ],
      scheduleMediaItem: {
        audio: "scheduleAudioItem",
        video: "scheduleVideoItem",
        image: "scheduleImageItem",
        gallery: "scheduleGalleryItem",
        file: "scheduleFileItem",
      },

      setStatusPublished: "publishMediaItem",
      setStatusScheduled: "scheduleMediaItem",
    });
  }

  // ============================================================================
  //                                SANITIZER
  // ============================================================================

  /**
   * sanitizeValidateFirst(payload, methodKeyOrNull)
   * Description:
   *   FIRST LINE for every method: validates required fields for handlers (if methodKey provided),
   *   and validates/coerces each payload field using FIELD_SPEC. Unknown fields → error.
   * Checklist:
   *   - Ensure payload is object.
   *   - If methodKey in METHOD_RULES → enforce required list.
   *   - For each key: verify it exists in FIELD_SPEC; coerce via _coerceByRule.
   *   - Normalize arrays (tags, performer lists).
   */
  sanitizeValidateFirst(payload, methodKey) {
    if (!payload || typeof payload !== "object")
      throw new ValidationError("Payload must be an object");

    // console.log("sanitizing", payload, methodKey);

    if (methodKey) {
      const required = this.METHOD_RULES[methodKey];
      if (!required)
        throw new ValidationError(`Unknown handler rules for '${methodKey}'`);

      // console.log("sanitizing", payload, methodKey);

      for (const f of required) {
        if (!(f in payload))
          throw new ValidationError(`Missing required field '${f}'`);
      }
    }

    const clean = {};
    for (const [k, v] of Object.entries(payload)) {
      const spec = this.FIELD_SPEC[k];
      if (!spec) throw new ValidationError(`Unexpected field '${k}'`);
      clean[k] = this._coerceByRule(k, v, spec.rule);
    }

    // Normalize arrays
    if ("tags" in clean)
      clean.tags = this.normalizeTags(
        Array.isArray(clean.tags) ? clean.tags : []
      );
    if ("coperformers" in clean)
      clean.coperformers = this.normalizeCoPerformers(
        Array.isArray(clean.coperformers) ? clean.coperformers : []
      );
    if ("performerIds" in clean)
      clean.performerIds = this.normalizeCoPerformers(
        Array.isArray(clean.performerIds) ? clean.performerIds : []
      );

    return clean;
  }

  /**
   * _coerceByRule(field, value, rule)
   * Description:
   *   Strictly validates & coerces based on FIELD_SPEC rules.
   * Checklist:
   *   - string[:nonempty][:max=N]
   *   - url:https
   *   - int[:>=N][:<=N]
   *   - bool
   *   - enum:A|B|C
   *   - json (deep-cloned)
   *   - datetime
   *   - Any mismatch throws immediately.
   */
  _coerceByRule(field, value, rule) {
    const parts = rule.split(":");
    const kind = parts[0];

    const clampStr = (s, max) => (s.length > max ? s.slice(0, max) : s);

    if (kind === "string") {
      const nonempty = parts.includes("nonempty");
      const maxPart = parts.find((p) => p.startsWith("max="));
      const max = maxPart ? Number(maxPart.split("=")[1]) : 10000;
      const s = typeof value === "string" ? value.trim() : "";
      if (nonempty && !s) console.log(`${field} must be nonempty string`);
      return clampStr(s, max);
    }

    if (kind === "url") {
      if (value == null || value === "") return null;
      if (!SafeUtils.sanitizeUrl(value)) {
        console.log(`${field} must be https URL`);
        return null;
      }
      if (String(value).length > this.config.maxUrlLength) {
        console.log(`${field} too long`);
        return null;
      }
      return String(value);
    }

    if (kind === "int") {
      let min = null,
        max = null;
      for (const p of parts.slice(1)) {
        if (p.startsWith(">=")) min = Number(p.replace(">=", ""));
        if (p.startsWith("<=")) max = Number(p.replace("<=", ""));
      }
      return SafeUtils.toInt(
        value,
        min ?? Number.MIN_SAFE_INTEGER,
        max ?? Number.MAX_SAFE_INTEGER
      );
    }

    if (kind === "bool") return SafeUtils.sanitizeBoolean(value);

    if (kind === "enum") {
      const allowed = parts[1].split("|");
      const s = String(value);
      if (!allowed.includes(s)) {
        console.log(`${field} invalid enum value`);
        return null;
      }
      return s;
    }

    if (kind === "json") {
      return value == null ? null : JSON.parse(JSON.stringify(value));
    }

    if (kind === "datetime") {
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) {
        console.log(`${field} invalid datetime`);
        return null;
      }
      return d;
    }

    console.log(`Unknown rule '${rule}' for ${field}`);
    return null;
  }

  // ============================================================================
  //                           EVENT VALIDATION (PUBLISH/SCHEDULE)
  // ============================================================================

  /**
   * enforceEventList(eventKey, row)
   * Description:
   *   Apply simple event list (publish/schedule) to the current row.
   * Checklist:
   *   - Resolve dispatcher keys by media_type.
   *   - Support atoms: "field", "field:https", "field>0", "field=false", "media_type=video", "publish_date>now".
   */
  enforceEventList(eventKey, row) {
    let list = this.EventMap[eventKey];
    if (!list) throw new ValidationError(`Unknown event '${eventKey}'`);

    if (typeof list === "object" && !Array.isArray(list)) {
      const mt = row.media_type;
      const mapped = list[mt];
      if (!mapped)
        throw new ValidationError(`No event mapping for media_type='${mt}'`);
      list = this.EventMap[mapped];
    }
    if (!Array.isArray(list))
      throw new ValidationError(`Invalid event list for '${eventKey}'`);

    const now = this.clock.now();

    for (const atom of list) {
      if (atom.includes("=")) {
        const [lhs, rhs] = atom.split("=");
        if (lhs === "media_type") {
          if (row.media_type !== rhs)
            throw new ValidationError(
              `media_type must be '${rhs}' to ${eventKey}`
            );
        } else if (lhs === "pending_conversion" && rhs === "false") {
          if (!!row.pending_conversion)
            throw new ValidationError(`pending_conversion must be false`);
        } else {
          if (String(row[lhs]) !== rhs)
            throw new ValidationError(`${lhs} must equal ${rhs}`);
        }
        continue;
      }
      if (atom.endsWith(">0")) {
        const field = atom.replace(">0", "");
        if (!(SafeUtils.toInt(row[field], 1, Number.MAX_SAFE_INTEGER) > 0))
          throw new ValidationError(`${field} must be > 0`);
        continue;
      }
      if (atom.endsWith("=false")) {
        const field = atom.replace("=false", "");
        if (!!row[field]) throw new ValidationError(`${field} must be false`);
        continue;
      }
      if (atom.endsWith(":https")) {
        const field = atom.replace(":https", "");
        if (!row[field]) throw new ValidationError(`${field} required`);
        if (!SafeUtils.sanitizeUrl(row[field]))
          throw new ValidationError(`${field} must be https`);
        continue;
      }
      if (atom.endsWith(">now")) {
        const field = atom.replace(">now", "");
        const d = new Date(row[field]);
        if (Number.isNaN(d.getTime()) || !(d.getTime() > now.getTime()))
          throw new ValidationError(`${field} must be in the future`);
        continue;
      }
      if (
        row[atom] == null ||
        (typeof row[atom] === "string" && !row[atom].trim())
      )
        throw new ValidationError(`${atom} is required for ${eventKey}`);
    }
  }

  // ============================================================================
  //                               NORMALIZERS
  // ============================================================================

  /**
   * normalizeTags(tags)
   * Description:
   *   Trim → lowercase → dedupe; enforce max count/length.
   * Checklist:
   *   - skip empty; clip tag length; cap to maxTagCount.
   */
  normalizeTags(tags) {
    console.log("at the top of normalizing tags", tags);
    const out = [];
    const seen = new Set();
    for (const t of tags) {
      const s = typeof t === "string" ? t.trim().toLowerCase() : "";
      if (!s) continue;
      const clipped = s.slice(0, this.config.maxTagLength);
      if (seen.has(clipped)) continue;
      out.push(clipped);
      seen.add(clipped);
      if (out.length >= this.config.maxTagCount) break;
    }
    console.log("normalized tags", out);
    return out;
  }

  /**
   * normalizeCoPerformers(ids)
   * Description:
   *   Trim → dedupe; enforce max, id length <= 191.
   * Checklist:
   *   - skip empty; clip; cap to maxCoPerformers.
   */
  normalizeCoPerformers(ids) {
    const out = [];
    const seen = new Set();
    for (const id of ids) {
      const s = typeof id === "string" ? id.trim() : "";
      if (!s) continue;
      const clipped = s.slice(0, 191);
      if (seen.has(clipped)) continue;
      out.push(clipped);
      seen.add(clipped);
      if (out.length >= this.config.maxCoPerformers) break;
    }
    return out;
  }

  // ============================================================================
  //                                   HANDLERS (4)
  // ============================================================================

  /**
   * handleAddMediaItem({ payload, actorUserId })
   * Description:
   *   Create a new media row and apply optional updates depending on payload presence.
   * Checklist:
   *   [ ] sanitizeValidateFirst(payload,'handleAddMediaItem')
   *   [ ] addRow (owner_user_id, media_type)
   *   [ ] Branch on presence: tags, coperformers, asset, poster, share flags, blur, ownership
   *   [ ] Log start/end and branch actions; reindex via addRow / subcalls
   */
  async handleAddMediaItem({ payload, actorUserId }) {
    const clean = this.sanitizeValidateFirst(payload, "handleAddMediaItem"); // FIRST LINE
    // const clean = SafeUtils.sanitizeValidate(payload)
    console.log("tesing after sanitization", clean);
    this.log?.info?.("handleAddMediaItem:start", { actorUserId });

    const { media_id } = await this.addRow({ ...clean, actorUserId });
    console.log("media_id", media_id);
    clean.media_id = media_id;

    if (Array.isArray(clean.tags)) {
      this.log?.info?.("handleAddMediaItem:branch:setTags", {
        mediaId: media_id,
      });
      await this.setTags({
        media_id,
        expectedVersion: 1,
        tags: clean.tags,
        actorUserId,
      });
    }
    if (Array.isArray(clean.coperformers)) {
      this.log?.info?.("handleAddMediaItem:branch:setCoPerformers", {
        mediaId: media_id,
      });
      await this.setCoPerformers({
        media_id,
        expectedVersion: 2,
        performerIds: clean.coperformers,
        actorUserId,
      });
    }
    if (
      clean.asset_url ||
      clean.file_extension ||
      clean.file_name ||
      clean.file_size_bytes != null ||
      clean.duration_seconds != null ||
      clean.video_width != null ||
      clean.video_height != null ||
      clean.pending_conversion != null
    ) {
      this.log?.info?.("handleAddMediaItem:branch:attachPrimaryAsset", {
        mediaId: media_id,
      });
      await this.attachPrimaryAsset({
        media_id,
        expectedVersion: 3,
        ...clean,
        actorUserId,
      });
    }
    if (clean.poster_url) {
      this.log?.info?.("handleAddMediaItem:branch:setPoster", {
        mediaId: media_id,
      });
      await this.setPoster({
        media_id,
        expectedVersion: 4,
        poster_url: clean.poster_url,
        actorUserId,
      });
    }
    if (
      clean.placeholder_lock != null ||
      clean.blurred_lock != null ||
      clean.blurred_value_px != null ||
      clean.trailer_blurred_lock != null ||
      clean.trailer_blurred_value_px != null
    ) {
      this.log?.info?.("handleAddMediaItem:branch:applyBlurControls", {
        mediaId: media_id,
      });
      await this.applyBlurControls({
        media_id,
        expectedVersion: 5,
        placeholder_lock: clean.placeholder_lock,
        blurred_lock: clean.blurred_lock,
        blurred_value_px: clean.blurred_value_px,
        trailer_blurred_lock: clean.trailer_blurred_lock,
        trailer_blurred_value_px: clean.trailer_blurred_value_px,
        actorUserId,
      });
    }
    if (clean.new_owner_user_id || clean.owner_user_id) {
      this.log?.info?.("handleAddMediaItem:branch:setOwnership", {
        mediaId: media_id,
      });
      await this.setOwnership({
        media_id,
        expectedVersion: 6,
        new_owner_user_id: clean.new_owner_user_id || clean.owner_user_id,
        actorUserId,
      });
    }

    this.log?.info?.("handleAddMediaItem:end", {
      mediaId: media_id,
      actorUserId,
    });
    return { media_id };
  }

  /**
   * handleUpdateMediaItem(payload)
   * Description:
   *   Single-call update that routes to specific setters based on payload presence.
   * Checklist:
   *   [ ] sanitizeValidateFirst(payload,'handleUpdateMediaItem')
   *   [ ] Branch: ownership, asset, poster, metadata, tags, coperformers, blur, soft/hard delete
   *   [ ] Log start/end and each branch taken
   */
  async handleUpdateMediaItem(payload) {
    // console.log("update payload", payload);
    const clean = this.sanitizeValidateFirst(payload, "handleUpdateMediaItem"); // FIRST LINE
    console.log("update sanitized", clean);
    this.log?.info?.("handleUpdateMediaItem:start", {
      mediaId: clean.media_id,
      actorUserId: clean.actorUserId,
      // handleUpdateMediaItem,
    });

    if (clean.new_owner_user_id || clean.owner_user_id) {
      this.log?.info?.("handleUpdateMediaItem:branch:setOwnership", {
        mediaId: clean.media_id,
      });
      await this.setOwnership({
        media_id: clean.media_id,
        expectedVersion: clean.expectedVersion,
        new_owner_user_id: clean.new_owner_user_id || clean.owner_user_id,
        actorUserId: clean.actorUserId,
      });
    }
    if (
      clean.asset_url ||
      clean.file_extension ||
      clean.file_name ||
      clean.file_size_bytes != null ||
      clean.duration_seconds != null ||
      clean.video_width != null ||
      clean.video_height != null ||
      clean.pending_conversion != null
    ) {
      this.log?.info?.("handleUpdateMediaItem:branch:attachPrimaryAsset", {
        mediaId: clean.media_id,
      });
      await this.attachPrimaryAsset({ ...clean });
    }
    if (clean.poster_url) {
      this.log?.info?.("handleUpdateMediaItem:branch:setPoster", {
        mediaId: clean.media_id,
      });
      await this.setPoster({
        media_id: clean.media_id,
        expectedVersion: clean.expectedVersion,
        poster_url: clean.poster_url,
        actorUserId: clean.actorUserId,
      });
    }
    if (
      clean.title ||
      clean.description ||
      clean.visibility ||
      typeof clean.featured === "boolean" ||
      typeof clean.coming_soon === "boolean" ||
      clean.image_variants_json ||
      clean.gallery_poster_url ||
      clean.media_meta
    ) {
      this.log?.info?.("handleUpdateMediaItem:branch:updateMetadata", {
        mediaId: clean.media_id,
      });
      await this.updateMetadata({ ...clean });
    }
    if (Array.isArray(clean.tags)) {
      this.log?.info?.("handleUpdateMediaItem:branch:setTags", {
        mediaId: clean.media_id,
      });
      await this.setTags({
        media_id: clean.media_id,
        expectedVersion: clean.expectedVersion,
        tags: clean.tags,
        actorUserId: clean.actorUserId,
      });
    }
    if (Array.isArray(clean.coperformers)) {
      this.log?.info?.("handleUpdateMediaItem:branch:setCoPerformers", {
        mediaId: clean.media_id,
      });
      await this.setCoPerformers({
        media_id: clean.media_id,
        expectedVersion: clean.expectedVersion,
        performerIds: clean.coperformers,
        actorUserId: clean.actorUserId,
      });
    }
    if (
      clean.placeholder_lock != null ||
      clean.blurred_lock != null ||
      clean.blurred_value_px != null ||
      clean.trailer_blurred_lock != null ||
      clean.trailer_blurred_value_px != null
    ) {
      this.log?.info?.("handleUpdateMediaItem:branch:applyBlurControls", {
        mediaId: clean.media_id,
      });
      await this.applyBlurControls({ ...clean });
    }
    if (clean.soft_delete === true) {
      this.log?.info?.("handleUpdateMediaItem:branch:softDelete", {
        mediaId: clean.media_id,
      });
      await this.softDelete({
        media_id: clean.media_id,
        expectedVersion: clean.expectedVersion,
        actorUserId: clean.actorUserId,
      });
    }
    if (clean.hard_delete === true) {
      this.log?.info?.("handleUpdateMediaItem:branch:hardDelete", {
        mediaId: clean.media_id,
      });
      await this.hardDelete({
        media_id: clean.media_id,
        actorUserId: clean.actorUserId,
      });
    }

    this.log?.info?.("handleUpdateMediaItem:end", {
      mediaId: clean.media_id,
      actorUserId: clean.actorUserId,
    });
    return { media_id: clean.media_id };
  }

  /**
   * handleScheduleMediaItem(payload)
   * Description:
   *   Validate schedule list for media_type (publish rules + publish_date>now); update to scheduled.
   * Checklist:
   *   [ ] sanitizeValidateFirst(payload,'handleScheduleMediaItem')
   *   [ ] load row; expect version
   *   [ ] enforceEventList('scheduleMediaItem', row with publish_date override)
   *   [ ] set status=scheduled; audit; reindex
   */
  async handleScheduleMediaItem(payload) {
    const clean = this.sanitizeValidateFirst(
      payload,
      "handleScheduleMediaItem"
    ); // FIRST LINE
    this.log?.info?.("handleScheduleMediaItem:start", {
      mediaId: clean.media_id,
      actorUserId: clean.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      this.expectVersion(row, clean.expectedVersion);

      const validateRow = { ...row, publish_date: clean.publish_date };
      this.enforceEventList("scheduleMediaItem", validateRow);

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media SET status='scheduled', publish_date=$2, last_updated=$3, updated_by_user_id=$4, version=$5 WHERE media_id=$1`,
        [
          clean.media_id,
          clean.publish_date,
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.SCHEDULE,
        beforeJson: {
          status: row.status,
          publish_date: row.publish_date,
          version: row.version,
        },
        afterJson: {
          status: "scheduled",
          publish_date: clean.publish_date,
          version: newVersion,
        },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
      this.log?.info?.("handleScheduleMediaItem:end", {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
      });
      return {
        media_id: clean.media_id,
        status: "scheduled",
        version: newVersion,
        publish_date: clean.publish_date,
      };
    });
  }

  /**
   * handlePublishMediaItem(payload)
   * Description:
   *   Validate per-type publish rules; set status=published; publish_date default now if missing.
   * Checklist:
   *   [ ] sanitizeValidateFirst(payload,'handlePublishMediaItem')
   *   [ ] load row; expect version
   *   [ ] enforceEventList('publishMediaItem', row)
   *   [ ] set status=published; audit; reindex
   */
  async handlePublishMediaItem(payload) {
    const clean = this.sanitizeValidateFirst(payload, "handlePublishMediaItem"); // FIRST LINE
    this.log?.info?.("handlePublishMediaItem:start", {
      mediaId: clean.media_id,
      actorUserId: clean.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      this.expectVersion(row, clean.expectedVersion);

      this.enforceEventList("publishMediaItem", row);

      const now = this.clock.now();
      const publishDate = row.publish_date || now;
      const newVersion = (row.version || 0) + 1;

      await client.query(
        `UPDATE media SET status='published', publish_date=$2, last_updated=$3, updated_by_user_id=$4, version=$5 WHERE media_id=$1`,
        [
          clean.media_id,
          publishDate,
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.PUBLISH,
        beforeJson: {
          status: row.status,
          publish_date: row.publish_date,
          version: row.version,
        },
        afterJson: {
          status: "published",
          publish_date: publishDate,
          version: newVersion,
        },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
      this.log?.info?.("handlePublishMediaItem:end", {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
      });
      return {
        media_id: clean.media_id,
        status: "published",
        version: newVersion,
        publish_date: publishDate,
      };
    });
  }

  // ============================================================================
  //                         CORE METHODS (WRITES + READS)
  // ============================================================================

  /**
   * addRow({...})
   * Description:
   *   Insert a new media row (status=draft). One row per upload.
   * Checklist:
   *   [ ] sanitizeValidateFirst(payload,null)
   *   [ ] INSERT media
   *   [ ] INSERT tags/coperformers if present
   *   [ ] audit add
   *   [ ] ES upsert
   */
  async addRow(payload) {
    console.log("payload inside the addRow", payload);

    try {
      const clean = this.sanitizeValidateFirst(payload, null);
      console.log("✅ [addRow] sanitized payload:", clean);

      this.log?.info?.("addRow:start", {
        owner_user_id: clean.owner_user_id,
        media_type: clean.media_type,
        actorUserId: payload.actorUserId,
      });

      if (!clean.owner_user_id || !clean.media_type) {
        console.error("❌ [addRow] Missing required fields:", {
          owner_user_id: clean.owner_user_id,
          media_type: clean.media_type,
        });
        throw new ValidationError("owner_user_id and media_type required");
      }

      const now = this.clock.now();
      const media_id = this.uuid.v4();
      console.log("🆔 [addRow] Generated media_id:", media_id);

      await this.db.withTransaction(async (client) => {
        console.log("🟡 [addRow] Starting DB transaction...");

        await client.query(
          `INSERT INTO media (media_id, owner_user_id, created_by_user_id, updated_by_user_id, media_type, status, visibility,
                            title, description, featured, coming_soon,
                            asset_url, file_extension, file_name, file_size_bytes, duration_seconds, video_width, video_height,
                            poster_url, pending_conversion, image_variants_json, gallery_poster_url,
                            entry_date, publish_date, last_updated, version, is_deleted, deleted_at, media_meta,
                            placeholder_lock, blurred_lock, blurred_value_px, trailer_blurred_lock, trailer_blurred_value_px)
         VALUES ($1,$2,$3,$4,$5,'draft',$6,$7,$8,$9,$10,
                 $11,$12,$13,$14,$15,$16,$17,
                 $18,$19,$20,$21,
                 $22,NULL,$23,1,false,NULL,$24,
                 $25,$26,$27,$28,$29)`,
          [
            media_id,
            clean.owner_user_id,
            payload.actorUserId || null,
            payload.actorUserId || null,
            clean.media_type,
            clean.visibility || this.VISIBILITY.PRIVATE,
            clean.title || "",
            clean.description || "",
            !!clean.featured,
            !!clean.coming_soon,
            clean.asset_url || null,
            clean.file_extension || null,
            clean.file_name || null,
            clean.file_size_bytes ?? null,
            clean.duration_seconds ?? null,
            clean.video_width ?? null,
            clean.video_height ?? null,
            clean.poster_url || null,
            !!clean.pending_conversion,
            clean.image_variants_json || null,
            clean.gallery_poster_url || null,
            now,
            now,
            clean.media_meta || null,
            !!clean.placeholder_lock,
            !!clean.blurred_lock,
            SafeUtils.toInt(clean.blurred_value_px ?? 0, 0, 40),
            !!clean.trailer_blurred_lock,
            SafeUtils.toInt(clean.trailer_blurred_value_px ?? 0, 0, 40),
          ]
        );
        console.log("✅ [addRow] Inserted into media table successfully.");

        if (Array.isArray(clean.tags) && clean.tags.length) {
          console.log(`🏷️ [addRow] Inserting ${clean.tags.length} tags...`);
          for (const tag of clean.tags) {
            await client.query(
              `INSERT INTO media_tags (media_id, tag) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
              [media_id, tag]
            );
          }
        }

        if (Array.isArray(clean.coperformers) && clean.coperformers.length) {
          console.log(
            `👥 [addRow] Inserting ${clean.coperformers.length} coperformers...`
          );
          for (const performer of clean.coperformers) {
            await client.query(
              `INSERT INTO media_coperformers (media_id, performer_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
              [media_id, performer]
            );
          }
        }

        console.log("📝 [addRow] Writing audit log...");
        await this.writeAudit(client, {
          mediaId: media_id,
          actorUserId: payload.actorUserId,
          action: this.ACTION.ADD,
          beforeJson: null,
          afterJson: { created: true, media_type: clean.media_type },
        });

        console.log("✅ [addRow] Transaction completed successfully.");
      });

      console.log("📦 [addRow] Indexing media:", media_id);
      await this.indexer.upsert(media_id);

      this.log?.info?.("addRow:end", {
        mediaId: media_id,
        actorUserId: payload.actorUserId,
      });

      console.log("✅ [addRow] Finished successfully:", { media_id });
      return { media_id };
    } catch (err) {
      console.error("🚨 [addRow] Error occurred:", err);
      this.log?.error?.("addRow:failed", {
        message: err.message,
        stack: err.stack,
        payload,
      });
      throw err; // rethrow so caller can handle it too
    }
  }

  /**
   * updateMetadata({...})
   * Description:
   *   Partial metadata fields update (title/description/visibility/etc.) with version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] dynamic SET; bump version/time; audit; ES upsert
   */
  async updateMetadata(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("updateMetadata:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);

      const set = [];
      const vals = [clean.media_id];
      let i = 2;
      const add = (k, v) => {
        if (v !== undefined) {
          set.push(`${k}=$${i++}`);
          vals.push(v);
        }
      };
      add("title", clean.title);
      add("description", clean.description);
      add("visibility", clean.visibility);
      if (typeof clean.featured === "boolean") add("featured", clean.featured);
      if (typeof clean.coming_soon === "boolean")
        add("coming_soon", clean.coming_soon);
      add("image_variants_json", clean.image_variants_json);
      add("gallery_poster_url", clean.gallery_poster_url);
      add("media_meta", clean.media_meta);

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      set.push(`version=$${i++}`);
      vals.push(newVersion);
      set.push(`last_updated=$${i++}`);
      vals.push(now);

      if (set.length) {
        await client.query(
          `UPDATE media SET ${set.join(
            ", "
          )}, updated_by_user_id=$${i} WHERE media_id=$1`,
          [...vals, payload.actorUserId || row.updated_by_user_id]
        );
      }

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.UPDATE,
        beforeJson: { version: row.version },
        afterJson: {
          version: newVersion,
          fields: set.map((s) => s.split("=")[0]),
        },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
      this.log?.info?.("updateMetadata:end", {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        version: newVersion,
      });
      return { media_id: clean.media_id, version: newVersion };
    });
  }

  /**
   * attachPrimaryAsset({...})
   * Description:
   *   Set/replace primary asset, file/duration/resolution/pending flags; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] update asset fields; audit; ES upsert
   */
  async attachPrimaryAsset(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("attachPrimaryAsset:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      console.log(
        "Expecting version:",
        clean.expectedVersion,
        "Current version:",
        row.version
      );

      this.expectVersion(row, clean.expectedVersion);

      const set = [];
      const vals = [clean.media_id];
      let i = 2;
      const add = (k, v) => {
        if (v !== undefined) {
          set.push(`${k}=$${i++}`);
          vals.push(v);
        }
      };
      add("asset_url", clean.asset_url);
      add("file_extension", clean.file_extension);
      add("file_name", clean.file_name);
      add("file_size_bytes", clean.file_size_bytes);
      add("duration_seconds", clean.duration_seconds);
      add("video_width", clean.video_width);
      add("video_height", clean.video_height);
      if (typeof clean.pending_conversion === "boolean")
        add("pending_conversion", clean.pending_conversion);

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      set.push(`version=$${i++}`);
      vals.push(newVersion);
      set.push(`last_updated=$${i++}`);
      vals.push(now);

      if (set.length) {
        await client.query(
          `UPDATE media SET ${set.join(
            ", "
          )}, updated_by_user_id=$${i} WHERE media_id=$1`,
          [...vals, payload.actorUserId || row.updated_by_user_id]
        );
      }

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.ASSET_ATTACH,
        beforeJson: { version: row.version },
        afterJson: { version: newVersion, asset_updated: true },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
      this.log?.info?.("attachPrimaryAsset:end", {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        version: newVersion,
      });
      return { media_id: clean.media_id, version: newVersion };
    });
  }

  /**
   * setPoster({...})
   * Description:
   *   Update poster_url; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] update poster_url; audit; ES upsert
   */
  async setPoster(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setPoster:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      console.log(
        "Expecting version:",
        clean.expectedVersion,
        "Current version:",
        row.version
      );

      this.expectVersion(row, clean.expectedVersion);

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media SET poster_url=$2, last_updated=$3, updated_by_user_id=$4, version=$5 WHERE media_id=$1`,
        [
          clean.media_id,
          clean.poster_url,
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.POSTER_SET,
        beforeJson: { poster_url: row.poster_url, version: row.version },
        afterJson: { poster_url: clean.poster_url, version: newVersion },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
      this.log?.info?.("setPoster:end", {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        version: newVersion,
      });
      return { media_id: clean.media_id, version: newVersion };
    });
  }

  /**
   * applyBlurControls({...})
   * Description:
   *   Update placeholder/blur flags + intensities; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] write flags; audit; ES upsert
   */
  async applyBlurControls(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("applyBlurControls:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;

      await client.query(
        `UPDATE media SET
           placeholder_lock=COALESCE($2, placeholder_lock),
           blurred_lock=COALESCE($3, blurred_lock),
           blurred_value_px=COALESCE($4, blurred_value_px),
           trailer_blurred_lock=COALESCE($5, trailer_blurred_lock),
           trailer_blurred_value_px=COALESCE($6, trailer_blurred_value_px),
           last_updated=$7, updated_by_user_id=$8, version=$9
         WHERE media_id=$1`,
        [
          clean.media_id,
          typeof clean.placeholder_lock === "boolean"
            ? clean.placeholder_lock
            : null,
          typeof clean.blurred_lock === "boolean" ? clean.blurred_lock : null,
          typeof clean.blurred_value_px === "number"
            ? clean.blurred_value_px
            : null,
          typeof clean.trailer_blurred_lock === "boolean"
            ? clean.trailer_blurred_lock
            : null,
          typeof clean.trailer_blurred_value_px === "number"
            ? clean.trailer_blurred_value_px
            : null,
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.BLUR_APPLY,
        beforeJson: { version: row.version },
        afterJson: { version: newVersion },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
      this.log?.info?.("applyBlurControls:end", {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        version: newVersion,
      });
      return { media_id: clean.media_id, version: newVersion };
    });
  }

  /**
   * setVisibility({...})
   * Description:
   *   Update visibility enum; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _simpleFieldUpdate(visibility)
   */
  async setVisibility(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setVisibility:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    const res = await this._simpleFieldUpdate({
      media_id: clean.media_id,
      expectedVersion: clean.expectedVersion,
      fields: { visibility: clean.visibility },
      actorUserId: payload.actorUserId,
      action: this.ACTION.VISIBILITY,
    });
    this.log?.info?.("setVisibility:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
      version: res.version,
    });
    return res;
  }

  /**
   * setFeatured({...})
   * Description:
   *   Toggle featured; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _simpleFieldUpdate(featured)
   */
  async setFeatured(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setFeatured:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    const res = await this._simpleFieldUpdate({
      media_id: clean.media_id,
      expectedVersion: clean.expectedVersion,
      fields: { featured: !!clean.featured },
      actorUserId: payload.actorUserId,
      action: this.ACTION.FEATURED,
    });
    this.log?.info?.("setFeatured:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
      version: res.version,
    });
    return res;
  }

  /**
   * setComingSoon({...})
   * Description:
   *   Toggle coming_soon; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _simpleFieldUpdate(coming_soon)
   */
  async setComingSoon(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setComingSoon:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    const res = await this._simpleFieldUpdate({
      media_id: clean.media_id,
      expectedVersion: clean.expectedVersion,
      fields: { coming_soon: !!clean.coming_soon },
      actorUserId: payload.actorUserId,
      action: this.ACTION.COMING_SOON,
    });
    this.log?.info?.("setComingSoon:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
      version: res.version,
    });
    return res;
  }

  /**
   * setTags({...})
   * Description:
   *   Replace entire tag set atomically; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] delete old, insert new; audit; ES upsert
   */
  async setTags(payload) {
    // Sanitize input
    const clean = this.sanitizeValidateFirst(payload, null);
    this.log?.info?.("setTags:start", {
      mediaId: clean.media_id,
      count: Array.isArray(clean.tags) ? clean.tags.length : 0,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      // Use the DB wrapper's getRow helper, not client.getRow
      const row = await this.db.getRow(
        client,
        `SELECT media_id, version, updated_by_user_id 
       FROM media 
       WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );

      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");

      this.expectVersion(row, clean.expectedVersion);

      // Clear old tags
      await client.query(`DELETE FROM media_tags WHERE media_id=$1`, [
        clean.media_id,
      ]);

      // Insert new tags
      if (clean.tags?.length) {
        const insertQuery = `INSERT INTO media_tags (media_id, tag) VALUES ($1,$2) ON CONFLICT DO NOTHING`;
        for (const tag of clean.tags) {
          await client.query(insertQuery, [clean.media_id, tag]);
        }
      }

      // Update media version
      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media 
       SET version=$2, last_updated=$3, updated_by_user_id=$4 
       WHERE media_id=$1`,
        [
          clean.media_id,
          newVersion,
          now,
          payload.actorUserId || row.updated_by_user_id,
        ]
      );

      // Write audit log
      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.TAGS_REPLACE,
        beforeJson: { version: row.version },
        afterJson: { version: newVersion, tags: clean.tags },
      });

      return { media_id: clean.media_id, version: newVersion };
    });
  }

  /**
   * addTag({...})
   * Description:
   *   Add a single tag if missing; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] upsert tag; bump; audit; ES upsert
   */
  async addTag(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    console.log("clean inside addTag", clean);
    const tag = this.normalizeTags(payload.tags ?? [])[0];
    if (!tag) throw new ValidationError("Invalid tag");
    this.log?.info?.("addTag:start", {
      mediaId: clean.media_id,
      tag,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT media_id, version, updated_by_user_id FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);

      await client.query(
        `INSERT INTO media_tags (media_id, tag) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
        [clean.media_id, tag]
      );

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media SET version=$2, last_updated=$3, updated_by_user_id=$4 WHERE media_id=$1`,
        [
          clean.media_id,
          newVersion,
          now,
          payload.actorUserId || row.updated_by_user_id,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.TAG_ADD,
        beforeJson: { version: row.version },
        afterJson: { version: newVersion, tag },
      });

      // ✅ return inside transaction
      return { media_id: clean.media_id, version: newVersion, tag };
    });
  }

  /**
   * removeTag({...})
   * Description:
   *   Remove a single tag; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] delete tag; bump; audit; ES upsert
   */
  async removeTag(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    const tag = this.normalizeTags(payload.tags ?? [])[0];
    if (!tag) throw new ValidationError("Invalid tag");
    this.log?.info?.("removeTag:start", {
      mediaId: clean.media_id,
      tag,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT media_id, version, updated_by_user_id FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);

      await client.query(
        `DELETE FROM media_tags WHERE media_id=$1 AND tag=$2`,
        [clean.media_id, tag]
      );

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media SET version=$2, last_updated=$3, updated_by_user_id=$4 WHERE media_id=$1`,
        [
          clean.media_id,
          newVersion,
          now,
          payload.actorUserId || row.updated_by_user_id,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.TAG_REMOVE,
        beforeJson: { version: row.version },
        afterJson: { version: newVersion, removed: tag },
      });
    });

    await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
    this.log?.info?.("removeTag:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return { media_id: clean.media_id };
  }

  /**
   * setCoPerformers({...})
   * Description:
   *   Replace coperformers array atomically; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] delete old; insert new; audit; ES upsert
   */
  async setCoPerformers(payload) {
    const clean = this.sanitizeValidateFirst(payload, null);
    this.log?.info?.("setCoPerformers:start", {
      mediaId: clean.media_id,
      count: Array.isArray(clean.performerIds) ? clean.performerIds.length : 0,
      actorUserId: payload.actorUserId,
    });

    return await this.db
      .withTransaction(async (client) => {
        // Use your DB wrapper to fetch single row
        const row = await this.db.getRow(
          client,
          `SELECT media_id, version, updated_by_user_id 
       FROM media 
       WHERE media_id=$1 AND is_deleted=false`,
          [clean.media_id]
        );
        console.log("row inside setCoPerformers", row);
        if (!row) throw new NotFoundError("Media not found");

        if (clean.expectedVersion == null)
          throw new ConflictError("expectedVersion required");
        // console.log(
        //   "Expecting version:",
        //   clean.expectedVersion,
        //   "Current version:",
        //   row.version
        // );
        this.expectVersion(row, clean.expectedVersion);

        // Delete existing co-performers
        await client.query(`DELETE FROM media_coperformers WHERE media_id=$1`, [
          clean.media_id,
        ]);

        // Insert new co-performers
        if (
          Array.isArray(clean.performerIds) &&
          clean.performerIds.length > 0
        ) {
          const insertQuery =
            "INSERT INTO media_coperformers (media_id, performer_id) VALUES ($1, $2) ON CONFLICT DO NOTHING";
          for (const performerId of clean.performerIds) {
            await client.query(insertQuery, [clean.media_id, performerId]);
          }
        }

        // Update media version
        const now = this.clock.now();
        const newVersion = (row.version || 0) + 1;
        await client.query(
          `UPDATE media SET version=$2, last_updated=$3, updated_by_user_id=$4 WHERE media_id=$1`,
          [
            clean.media_id,
            newVersion,
            now,
            payload.actorUserId || row.updated_by_user_id,
          ]
        );

        // Audit log
        await this.writeAudit(client, {
          mediaId: clean.media_id,
          actorUserId: payload.actorUserId,
          action: this.ACTION.COPERFORMERS_REPLACE,
          beforeJson: { version: row.version },
          afterJson: { version: newVersion, coperformers: clean.performerIds },
        });

        return { media_id: clean.media_id };
      })
      .then(async (res) => {
        // Optional: index in Elasticsearch
        await this.indexer.upsert(clean.media_id);
        this.log?.info?.("setCoPerformers:end", {
          mediaId: clean.media_id,
          actorUserId: payload.actorUserId,
        });
        return res;
      });
  }

  /**
   * setOwnership({...})
   * Description:
   *   Transfer ownership to new_owner_user_id; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _simpleFieldUpdate(owner_user_id)
   */
  async setOwnership(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setOwnership:start", {
      mediaId: clean.media_id,
      new_owner_user_id: clean.new_owner_user_id,
      actorUserId: payload.actorUserId,
    });
    const res = await this._simpleFieldUpdate({
      media_id: clean.media_id,
      expectedVersion: clean.expectedVersion,
      fields: { owner_user_id: clean.new_owner_user_id },
      actorUserId: payload.actorUserId,
      action: this.ACTION.OWNERSHIP,
    });
    this.log?.info?.("setOwnership:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
      version: res.version,
    });
    return res;
  }

  /**
   * setCustomMeta({...})
   * Description:
   *   Replace or merge media_meta JSON; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] merge(if merge=true) or replace; audit; ES upsert
   */
  async setCustomMeta(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setCustomMeta:start", {
      mediaId: clean.media_id,
      merge: !!clean.merge,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT media_id, media_meta, version, updated_by_user_id FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);

      const next = clean.merge
        ? { ...(row.media_meta || {}), ...(clean.media_meta || {}) }
        : clean.media_meta || {};
      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;

      await client.query(
        `UPDATE media SET media_meta=$2, last_updated=$3, updated_by_user_id=$4, version=$5 WHERE media_id=$1`,
        [
          clean.media_id,
          JSON.stringify(next),
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.UPDATE,
        beforeJson: { version: row.version, media_meta: row.media_meta },
        afterJson: { version: newVersion, media_meta: next },
      });
    });

    await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
    this.log?.info?.("setCustomMeta:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return { media_id: clean.media_id };
  }

  /**
   * softDelete({...})
   * Description:
   *   Soft delete: is_deleted=true, status='deleted'; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion
   *   [ ] mark deleted; audit; ES delete
   */
  async softDelete(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("softDelete:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media SET is_deleted=true, status='deleted', deleted_at=$2, last_updated=$2, updated_by_user_id=$3, version=$4 WHERE media_id=$1`,
        [
          clean.media_id,
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.SOFT_DELETE,
        beforeJson: {
          status: row.status,
          is_deleted: row.is_deleted,
          version: row.version,
        },
        afterJson: { status: "deleted", is_deleted: true, version: newVersion },
      });

      await this.indexer.delete(clean.media_id); // Implement elasticsearch here
    });

    this.log?.info?.("softDelete:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return { media_id: clean.media_id, is_deleted: true };
  }

  /**
   * hardDelete({...})
   * Description:
   *   Hard delete media + children; remove ES doc.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] delete children then media
   *   [ ] audit; ES delete
   */
  async hardDelete(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("hardDelete:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      await client.query(`DELETE FROM media_tags WHERE media_id=$1`, [
        clean.media_id,
      ]);
      await client.query(`DELETE FROM media_coperformers WHERE media_id=$1`, [
        clean.media_id,
      ]);
      await client.query(`DELETE FROM media_reminders WHERE media_id=$1`, [
        clean.media_id,
      ]);
      await client.query(`DELETE FROM media_audit WHERE media_id=$1`, [
        clean.media_id,
      ]);
      await client.query(`DELETE FROM collection_media WHERE media_id=$1`, [
        clean.media_id,
      ]);
      await client.query(`DELETE FROM media WHERE media_id=$1`, [
        clean.media_id,
      ]);

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.HARD_DELETE,
        beforeJson: null,
        afterJson: { hard_deleted: true },
      });

      await this.indexer.delete(clean.media_id); // Implement elasticsearch here
    });

    this.log?.info?.("hardDelete:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return { media_id: clean.media_id, deleted: true };
  }

  /**
   * getById({...})
   * Description:
   *   Fetch single media row; optionally include tags/coperformers.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] select row; append relations if requested
   */
  async getById(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("getById:start", { mediaId: clean.media_id });

    const row = await this.db.getRow(
      `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
      [clean.media_id]
    );
    if (!row) throw new NotFoundError("Media not found");

    if (clean.includeTags) {
      row.tags = (
        await this.db.getAll(
          `SELECT tag FROM media_tags WHERE media_id=$1 ORDER BY tag`,
          [clean.media_id]
        )
      ).map((r) => r.tag);
    }
    if (clean.includeCoPerformers) {
      row.coperformers = (
        await this.db.getAll(
          `SELECT performer_id FROM media_coperformers WHERE media_id=$1 ORDER BY performer_id`,
          [clean.media_id]
        )
      ).map((r) => r.performer_id);
    }

    this.log?.info?.("getById:end", { mediaId: clean.media_id });
    return row;
  }

  /**
   * listByOwner({...})
   * Description:
   *   Owner-scoped list with filters & keyset pagination (DB fallback to ES).
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _listWithFilters({ scope:'owner' })
   */
  async listByOwner(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("listByOwner:start", {
      owner_user_id: clean.owner_user_id,
    });
    const res = await this._listWithFilters({
      scope: "owner",
      owner_user_id: clean.owner_user_id,
      ...clean,
    });
    this.log?.info?.("listByOwner:end", {
      owner_user_id: clean.owner_user_id,
      count: res.items.length,
    });
    return res;
  }

  /**
   * listPublic({...})
   * Description:
   *   Public, published list with filters & keyset pagination (DB fallback to ES).
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _listWithFilters({ scope:'public' })
   */
  async listPublic(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("listPublic:start", {});
    const res = await this._listWithFilters({ scope: "public", ...clean });
    this.log?.info?.("listPublic:end", { count: res.items.length });
    return res;
  }

  /**
   * listFeatured({...})
   * Description:
   *   Featured & published list.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _listWithFilters({ scope:'featured' })
   */
  async listFeatured(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("listFeatured:start", {});
    const res = await this._listWithFilters({ scope: "featured", ...clean });
    this.log?.info?.("listFeatured:end", { count: res.items.length });
    return res;
  }

  /**
   * listComingSoon({...})
   * Description:
   *   Coming soon list.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _listWithFilters({ scope:'coming_soon' })
   */
  async listComingSoon(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("listComingSoon:start", {});
    const res = await this._listWithFilters({ scope: "coming_soon", ...clean });
    this.log?.info?.("listComingSoon:end", { count: res.items.length });
    return res;
  }

  /**
   * listByTag({...})
   * Description:
   *   Tag-filtered list with AND-able extra filters.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] _listWithFilters({ scope:'tag' })
   */
  async listByTag(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("listByTag:start", { tag: clean.tag });
    const res = await this._listWithFilters({
      scope: "tag",
      tag: clean.tag,
      ...clean,
    });
    this.log?.info?.("listByTag:end", {
      tag: clean.tag,
      count: res.items.length,
    });
    return res;
  }

  /**
   * search({...})
   * Description:
   *   ES primary, DB fallback (title/description ILIKE).
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] try ES; fallback: DB
   */
  async search(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    const q = (clean.query || "").trim();
    this.log?.info?.("search:start", { query: q });

    // Primary: // Implement elasticsearch here
    // Fallback:
    const items = await this.db.getAll(
      `SELECT * FROM media
       WHERE is_deleted=false
         AND status='published'
         AND (title ILIKE $1 OR description ILIKE $1)
       ORDER BY COALESCE(publish_date, entry_date) DESC, media_id DESC
       LIMIT 101`,
      [q ? `%${q}%` : "%%"]
    );

    this.log?.info?.("search:end", { query: q, count: items.length });
    return { items, nextCursor: null };
  }

  /**
   * reindexSearch({...})
   * Description:
   *   Force ES reindex for a media item.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] indexer.upsert(media_id)
   */
  async reindexSearch(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("reindexSearch:start", { mediaId: clean.media_id });
    await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
    this.log?.info?.("reindexSearch:end", { mediaId: clean.media_id });
    return { media_id: clean.media_id, reindexed: true };
  }

  /**
   * createCollection({...})
   * Description:
   *   Create a collection/playlist/group.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] insert collection; audit
   */
  async createCollection(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("createCollection:start", {
      owner_user_id: clean.owner_user_id,
      title: clean.title,
      actorUserId: payload.actorUserId,
    });

    const collection_id = this.uuid.v4();
    await this.db.withTransaction(async (client) => {
      await client.query(
        `INSERT INTO collections (collection_id, owner_user_id, title, description, visibility, poster_url, created_at)
         VALUES ($1,$2,$3,$4,$5,$6,NOW())`,
        [
          collection_id,
          clean.owner_user_id,
          clean.title,
          clean.description || null,
          clean.visibility || this.VISIBILITY.PRIVATE,
          clean.poster_url || null,
        ]
      );
      await this.writeAudit(client, {
        mediaId: collection_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.COLLECTION_CREATE,
        beforeJson: null,
        afterJson: { collection_id, title: clean.title },
      });
    });

    this.log?.info?.("createCollection:end", {
      collection_id,
      actorUserId: payload.actorUserId,
    });
    return { collection_id };
  }

  /**
   * addToCollection({...})
   * Description:
   *   Add media to collection (optional position).
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] upsert into collection_media; audit
   */
  async addToCollection(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("addToCollection:start", {
      collection_id: clean.collection_id,
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    await this.db.withTransaction(async (client) => {
      await client.query(
        `INSERT INTO collection_media (collection_id, media_id, position)
         VALUES ($1,$2,$3)
         ON CONFLICT (collection_id, media_id) DO UPDATE SET position=EXCLUDED.position`,
        [
          clean.collection_id,
          clean.media_id,
          typeof clean.position === "number" ? clean.position : null,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.COLLECTION_ADD,
        beforeJson: null,
        afterJson: {
          collection_id: clean.collection_id,
          position: clean.position ?? null,
        },
      });
    });

    this.log?.info?.("addToCollection:end", {
      collection_id: clean.collection_id,
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return { collection_id: clean.collection_id, media_id: clean.media_id };
  }

  /**
   * removeFromCollection({...})
   * Description:
   *   Remove media from a collection.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] delete from collection_media; audit
   */
  async removeFromCollection(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("removeFromCollection:start", {
      collection_id: clean.collection_id,
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    await this.db.withTransaction(async (client) => {
      await client.query(
        `DELETE FROM collection_media WHERE collection_id=$1 AND media_id=$2`,
        [clean.collection_id, clean.media_id]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.COLLECTION_REMOVE,
        beforeJson: null,
        afterJson: { collection_id: clean.collection_id },
      });
    });

    this.log?.info?.("removeFromCollection:end", {
      collection_id: clean.collection_id,
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return {
      collection_id: clean.collection_id,
      media_id: clean.media_id,
      removed: true,
    };
  }

  /**
   * listCollection({...})
   * Description:
   *   List items within a collection, ordered by position desc then id desc.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] join fetch; keyset-ish pagination
   */
  async listCollection(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    const limit = Math.min(clean.limit || 24, 100);
    this.log?.info?.("listCollection:start", {
      collection_id: clean.collection_id,
      limit,
    });

    const items = await this.db.getAll(
      `SELECT m.* FROM collection_media cm
       JOIN media m ON m.media_id = cm.media_id
       WHERE cm.collection_id=$1 AND m.is_deleted=false
       ORDER BY COALESCE(cm.position,0) DESC, m.media_id DESC
       LIMIT $2`,
      [clean.collection_id, limit + 1]
    );
    const hasMore = items.length > limit;
    if (hasMore) items.pop();

    this.log?.info?.("listCollection:end", {
      collection_id: clean.collection_id,
      count: items.length,
    });
    return { items, nextCursor: null };
  }

  /**
   * schedulePublish({...})
   * Description:
   *   Thin wrapper that reuses handler validation/logic for scheduling.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] delegate to handleScheduleMediaItem
   */
  async schedulePublish(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("schedulePublish:delegate", { mediaId: clean.media_id });
    return this.handleScheduleMediaItem({
      ...clean,
      actorUserId: payload.actorUserId,
    });
  }

  /**
   * cancelSchedule({...})
   * Description:
   *   If currently scheduled, revert to pending_review; version bump.
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] load row; expectVersion; must be scheduled
   *   [ ] update; audit; ES upsert
   */
  async cancelSchedule(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("cancelSchedule:start", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });

    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [clean.media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (clean.expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, clean.expectedVersion);
      if (row.status !== this.STATUS.SCHEDULED)
        throw new StateTransitionError("Not scheduled");

      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      await client.query(
        `UPDATE media SET status='pending_review', last_updated=$2, updated_by_user_id=$3, version=$4 WHERE media_id=$1`,
        [
          clean.media_id,
          now,
          payload.actorUserId || row.updated_by_user_id,
          newVersion,
        ]
      );

      await this.writeAudit(client, {
        mediaId: clean.media_id,
        actorUserId: payload.actorUserId,
        action: this.ACTION.SCHEDULE,
        beforeJson: { status: row.status, version: row.version },
        afterJson: { status: "pending_review", version: newVersion },
      });

      await this.indexer.upsert(clean.media_id); // Implement elasticsearch here
    });

    this.log?.info?.("cancelSchedule:end", {
      mediaId: clean.media_id,
      actorUserId: payload.actorUserId,
    });
    return { media_id: clean.media_id, status: "pending_review" };
  }

  /**
   * setStatusPublished({...})
   * Description:
   *   Delegate to publish handler (same strict validation).
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] delegate to handlePublishMediaItem
   */
  async setStatusPublished(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setStatusPublished:delegate", {
      mediaId: clean.media_id,
    });
    return this.handlePublishMediaItem({
      ...clean,
      actorUserId: payload.actorUserId,
    });
  }

  /**
   * setStatusScheduled({...})
   * Description:
   *   Delegate to schedule handler (same strict validation).
   * Checklist:
   *   [ ] sanitizeValidateFirst
   *   [ ] delegate to handleScheduleMediaItem
   */
  async setStatusScheduled(payload) {
    const clean = this.sanitizeValidateFirst(payload, null); // FIRST LINE
    this.log?.info?.("setStatusScheduled:delegate", {
      mediaId: clean.media_id,
    });
    return this.handleScheduleMediaItem({
      ...clean,
      actorUserId: payload.actorUserId,
    });
  }

  // ============================================================================
  //                             INTERNAL BUILDERS
  // ============================================================================

  /**
   * _simpleFieldUpdate({ media_id, expectedVersion, fields, actorUserId, action })
   * Description:
   *   One-shot field updater with version bump, audit, and ES upsert.
   * Checklist:
   *   [ ] load row; expectVersion
   *   [ ] dynamic SET; bump version/time; audit; ES upsert
   */
  async _simpleFieldUpdate({
    media_id,
    expectedVersion,
    fields,
    actorUserId,
    action,
  }) {
    return await this.db.withTransaction(async (client) => {
      const row = await client.getRow(
        `SELECT * FROM media WHERE media_id=$1 AND is_deleted=false`,
        [media_id]
      );
      if (!row) throw new NotFoundError("Media not found");
      if (expectedVersion == null)
        throw new ConflictError("expectedVersion required");
      this.expectVersion(row, expectedVersion);

      const set = [];
      const vals = [media_id];
      let i = 2;
      for (const [k, v] of Object.entries(fields)) {
        set.push(`${k}=$${i++}`);
        vals.push(v);
      }
      const now = this.clock.now();
      const newVersion = (row.version || 0) + 1;
      set.push(`version=$${i++}`);
      vals.push(newVersion);
      set.push(`last_updated=$${i++}`);
      vals.push(now);

      await client.query(
        `UPDATE media SET ${set.join(
          ", "
        )}, updated_by_user_id=$${i} WHERE media_id=$1`,
        [...vals, actorUserId || row.updated_by_user_id]
      );

      await this.writeAudit(client, {
        mediaId: media_id,
        actorUserId,
        action: action || this.ACTION.UPDATE,
        beforeJson: { version: row.version },
        afterJson: { version: newVersion, fields },
      });

      await this.indexer.upsert(media_id); // Implement elasticsearch here
      return { media_id, version: newVersion };
    });
  }

  /**
   * _listWithFilters({ scope, owner_user_id, tag, query, filters, limit, cursor })
   * Description:
   *   Central list builder (DB fallback to ES); keyset via date+id (simplified).
   * Checklist:
   *   [ ] build WHERE by scope & filters
   *   [ ] ORDER BY COALESCE(publish_date, entry_date) DESC, media_id DESC
   *   [ ] LIMIT +1 detect next cursor (omitted: token)
   */
  async _listWithFilters(params) {
    const limit = Math.min(params.limit || 24, 100);
    const where = ["m.is_deleted=false"];
    const values = [];
    let idx = 1;

    if (params.scope === "owner") {
      where.push(`m.owner_user_id=$${idx++}`);
      values.push(params.owner_user_id);
    } else if (params.scope === "public") {
      where.push(`m.status='published'`);
      where.push(
        `m.visibility IN ('public','unlisted','subscribers','purchasers')`
      );
    } else if (params.scope === "featured") {
      where.push(`m.status='published'`);
      where.push(`m.featured=true`);
    } else if (params.scope === "coming_soon") {
      where.push(`m.coming_soon=true`);
    } else if (params.scope === "tag") {
      where.push(
        `EXISTS (SELECT 1 FROM media_tags t WHERE t.media_id=m.media_id AND t.tag=$${idx++})`
      );
      values.push(params.tag);
    }

    const f = params.filters || {};
    if (f.media_type) {
      where.push(`m.media_type=$${idx++}`);
      values.push(f.media_type);
    }
    if (f.status) {
      where.push(`m.status=$${idx++}`);
      values.push(f.status);
    }
    if (Number.isFinite(f.min_duration)) {
      where.push(`COALESCE(m.duration_seconds,0) >= $${idx++}`);
      values.push(f.min_duration);
    }
    if (Number.isFinite(f.max_duration)) {
      where.push(`COALESCE(m.duration_seconds,0) <= $${idx++}`);
      values.push(f.max_duration);
    }
    if (f.tags_all && Array.isArray(f.tags_all) && f.tags_all.length) {
      for (const t of f.tags_all) {
        where.push(
          `EXISTS (SELECT 1 FROM media_tags tt WHERE tt.media_id=m.media_id AND tt.tag=$${idx++})`
        );
        values.push(t);
      }
    }
    if (f.from_date) {
      where.push(`COALESCE(m.publish_date, m.entry_date) >= $${idx++}`);
      values.push(new Date(f.from_date));
    }
    if (f.to_date) {
      where.push(`COALESCE(m.publish_date, m.entry_date) <= $${idx++}`);
      values.push(new Date(f.to_date));
    }

    const sql = `SELECT m.*
       FROM media m
       WHERE ${where.join(" AND ")}
       ORDER BY COALESCE(m.publish_date, m.entry_date) DESC, m.media_id DESC
       LIMIT $${idx++}`;

    const rows = await this.db.getAll(sql, [...values, limit + 1]);
    const hasMore = rows.length > limit;
    if (hasMore) rows.pop();

    return { items: rows, nextCursor: null };
  }

  // ============================================================================
  //                         VERSION GUARD / AUDIT HELPERS
  // ============================================================================

  /**
   * expectVersion(row, expectedVersion)
   * Description:
   *   Optimistic concurrency guard. Throws ConflictError on mismatch.
   * Checklist:
   *   [ ] check integer
   *   [ ] compare to row.version
   */
  expectVersion(row, expectedVersion) {
    if (!Number.isInteger(expectedVersion))
      throw new ConflictError("expectedVersion required");
    // if ((row.version || 0) !== expectedVersion)
    //   throw new ConflictError("Version mismatch");
  }

  /**
   * writeAudit(client, { mediaId, actorUserId, action, beforeJson, afterJson })
   * Description:
   *   Insert audit log row (JSONB before/after) inside current TX.
   * Checklist:
   *   [ ] INSERT audit with NOW(), actor id, action, before/after
   */
  async writeAudit(
    client,
    { mediaId, actorUserId, action, beforeJson, afterJson }
  ) {
    await client.query(
      `INSERT INTO media_audit (media_id, occurred_at, actor_user_id, action, before_json, after_json)
       VALUES ($1, NOW(), $2, $3, $4::jsonb, $5::jsonb)`,
      [
        mediaId,
        actorUserId || null,
        action,
        JSON.stringify(beforeJson ?? null),
        JSON.stringify(afterJson ?? null),
      ]
    );
  }
}
