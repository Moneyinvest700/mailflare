import { B2Object } from "@/lib/storage/b2-bucket";
import type { B2HttpMetadata } from "@/lib/storage/b2-types";

/**
 * Tab patch (Nov 3 2026, tasmiya-mail deploy): the R2 bucket API slice served by a D1 database (binding BLOB_DB),
 * for free Cloudflare accounts with no R2 and no external S3. Objects are chunked into BLOB rows.
 * Active only when the BLOB_DB binding exists; other deploys are unaffected.
 */
type PutValue = ArrayBuffer | ArrayBufferView | string | ReadableStream | Blob | null;
type PutOptions = { httpMetadata?: B2HttpMetadata | Headers; customMetadata?: Record<string, string> };
type Meta = { http: B2HttpMetadata; custom: Record<string, string> };

const CHUNK = 512 * 1024;

async function toBytes(value: PutValue): Promise<Uint8Array> {
	if (value === null) return new Uint8Array(0);
	if (typeof value === "string") return new TextEncoder().encode(value);
	if (value instanceof ArrayBuffer) return new Uint8Array(value);
	if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
	if (value instanceof Blob) return new Uint8Array(await value.arrayBuffer());
	return new Uint8Array(await new Response(value).arrayBuffer());
}

function toMeta(options?: PutOptions): Meta {
	const m = options?.httpMetadata;
	const http: B2HttpMetadata = m instanceof Headers
		? { contentType: m.get("content-type") ?? undefined, contentDisposition: m.get("content-disposition") ?? undefined, cacheControl: m.get("cache-control") ?? undefined }
		: { ...(m ?? {}) };
	return { http, custom: { ...(options?.customMetadata ?? {}) } };
}

async function etagOf(bytes: Uint8Array): Promise<string> {
	const d = new Uint8Array(await crypto.subtle.digest("SHA-1", bytes));
	return Array.from(d.slice(0, 16), (b) => b.toString(16).padStart(2, "0")).join("");
}

function headersFor(meta: Meta, etag: string, uploaded: number, size: number): Headers {
	const h = new Headers();
	if (meta.http.contentType) h.set("content-type", meta.http.contentType);
	if (meta.http.contentDisposition) h.set("content-disposition", meta.http.contentDisposition);
	if (meta.http.cacheControl) h.set("cache-control", meta.http.cacheControl);
	for (const [k, v] of Object.entries(meta.custom)) h.set(`x-amz-meta-${k.toLowerCase()}`, v);
	h.set("etag", `"${etag}"`);
	h.set("last-modified", new Date(uploaded).toUTCString());
	h.set("content-length", String(size));
	return h;
}

export class D1Bucket {
	private ready: Promise<void> | null = null;
	constructor(private readonly db: D1Database) {}

	private init() {
		if (!this.ready) {
			this.ready = this.db.batch([
				this.db.prepare("CREATE TABLE IF NOT EXISTS blob_objects (key TEXT PRIMARY KEY, size INTEGER NOT NULL, etag TEXT NOT NULL, uploaded INTEGER NOT NULL, meta TEXT NOT NULL, chunks INTEGER NOT NULL)"),
				this.db.prepare("CREATE TABLE IF NOT EXISTS blob_chunks (key TEXT NOT NULL, idx INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (key, idx))"),
			]).then(() => undefined).catch((e) => { this.ready = null; throw e; });
		}
		return this.ready;
	}

	private async writeObject(key: string, bytes: Uint8Array, meta: Meta) {
		await this.init();
		const etag = await etagOf(bytes);
		const uploaded = Date.now();
		const n = Math.ceil(bytes.byteLength / CHUNK);
		await this.db.prepare("DELETE FROM blob_chunks WHERE key = ?").bind(key).run();
		for (let i = 0; i < n; i += 4) {
			const stmts = [];
			for (let j = i; j < Math.min(n, i + 4); j++) {
				stmts.push(this.db.prepare("INSERT INTO blob_chunks (key, idx, data) VALUES (?, ?, ?)").bind(key, j, bytes.slice(j * CHUNK, (j + 1) * CHUNK).buffer));
			}
			await this.db.batch(stmts);
		}
		await this.db.prepare("INSERT OR REPLACE INTO blob_objects (key, size, etag, uploaded, meta, chunks) VALUES (?, ?, ?, ?, ?, ?)")
			.bind(key, bytes.byteLength, etag, uploaded, JSON.stringify(meta), n).run();
		return { etag, uploaded, size: bytes.byteLength };
	}

	private async row(key: string) {
		await this.init();
		return this.db.prepare("SELECT size, etag, uploaded, meta, chunks FROM blob_objects WHERE key = ?").bind(key)
			.first<{ size: number; etag: string; uploaded: number; meta: string; chunks: number }>();
	}

	private async readBytes(key: string): Promise<Uint8Array> {
		const { results } = await this.db.prepare("SELECT data FROM blob_chunks WHERE key = ? ORDER BY idx").bind(key).all<{ data: ArrayBuffer | number[] }>();
		const parts = (results ?? []).map((r) => (r.data instanceof ArrayBuffer ? new Uint8Array(r.data) : new Uint8Array(r.data as number[])));
		const total = parts.reduce((s, p) => s + p.byteLength, 0);
		const out = new Uint8Array(total);
		let off = 0;
		for (const p of parts) { out.set(p, off); off += p.byteLength; }
		return out;
	}

	async head(key: string) {
		const r = await this.row(key);
		if (!r) return null;
		return new B2Object(key, new Response(null, { headers: headersFor(JSON.parse(r.meta), r.etag, r.uploaded, r.size) }), r.size);
	}

	async get(key: string, options?: { range?: { offset?: number; length?: number; suffix?: number } }) {
		const r = await this.row(key);
		if (!r) return null;
		let bytes = await this.readBytes(key);
		const range = options?.range;
		const h = headersFor(JSON.parse(r.meta), r.etag, r.uploaded, r.size);
		if (range) {
			let start = 0; let end = bytes.byteLength;
			if (range.suffix != null) start = Math.max(0, bytes.byteLength - range.suffix);
			else { start = range.offset ?? 0; if (range.length != null) end = Math.min(end, start + range.length); }
			bytes = bytes.slice(start, end);
			h.set("content-length", String(bytes.byteLength));
			h.set("content-range", `bytes ${start}-${Math.max(start, end - 1)}/${r.size}`);
		}
		return new B2Object(key, new Response(bytes, { headers: h }), r.size);
	}

	async put(key: string, value: PutValue, options?: PutOptions) {
		const bytes = await toBytes(value);
		const meta = toMeta(options);
		const w = await this.writeObject(key, bytes, meta);
		return new B2Object(key, new Response(null, { headers: headersFor(meta, w.etag, w.uploaded, w.size) }), w.size);
	}

	async delete(keys: string | string[]) {
		await this.init();
		const list = Array.isArray(keys) ? keys : [keys];
		for (const key of list) {
			await this.db.batch([
				this.db.prepare("DELETE FROM blob_chunks WHERE key = ?").bind(key),
				this.db.prepare("DELETE FROM blob_objects WHERE key = ?").bind(key),
			]);
		}
	}

	async list(options: { prefix?: string; cursor?: string; limit?: number } = {}) {
		await this.init();
		const limit = Math.min(options.limit ?? 1000, 1000);
		const prefix = options.prefix ?? "";
		const after = options.cursor ?? "";
		const { results } = await this.db.prepare("SELECT key, size, uploaded FROM blob_objects WHERE substr(key, 1, ?) = ? AND key > ? ORDER BY key LIMIT ?")
			.bind(prefix.length, prefix, after, limit + 1).all<{ key: string; size: number; uploaded: number }>();
		const rows = results ?? [];
		const truncated = rows.length > limit;
		const page = rows.slice(0, limit);
		return {
			objects: page.map((r) => ({ key: r.key, size: r.size, uploaded: new Date(r.uploaded) })),
			truncated,
			cursor: truncated ? page[page.length - 1].key : undefined,
		};
	}

	async createMultipartUpload(key: string, options?: { httpMetadata?: B2HttpMetadata; customMetadata?: Record<string, string> }) {
		await this.init();
		const uploadId = crypto.randomUUID();
		await this.db.prepare("INSERT OR REPLACE INTO blob_objects (key, size, etag, uploaded, meta, chunks) VALUES (?, 0, '', ?, ?, 0)")
			.bind(`__mpu/${uploadId}`, Date.now(), JSON.stringify({ key, ...toMeta(options) })).run();
		return this.resumeMultipartUpload(key, uploadId);
	}

	resumeMultipartUpload(key: string, uploadId: string) {
		const partKey = (n: number) => `__mpu/${uploadId}/${String(n).padStart(6, "0")}`;
		return {
			key,
			uploadId,
			uploadPart: async (partNumber: number, value: PutValue) => {
				if (!Number.isInteger(partNumber) || partNumber < 1) throw new Error("Invalid part number");
				const w = await this.writeObject(partKey(partNumber), await toBytes(value), { http: {}, custom: {} });
				return { partNumber, etag: w.etag };
			},
			abort: async () => {
				const l = await this.list({ prefix: `__mpu/${uploadId}` });
				await this.delete(l.objects.map((o) => o.key));
			},
			complete: async (parts: { partNumber: number; etag: string }[]) => {
				const head = await this.row(`__mpu/${uploadId}`);
				const stored = head ? (JSON.parse(head.meta) as Meta) : { http: {}, custom: {} };
				const meta: Meta = { http: stored.http ?? {}, custom: stored.custom ?? {} };
				const chunks: Uint8Array[] = [];
				for (const p of [...parts].sort((a, b) => a.partNumber - b.partNumber)) chunks.push(await this.readBytes(partKey(p.partNumber)));
				const total = chunks.reduce((s, c) => s + c.byteLength, 0);
				const all = new Uint8Array(total);
				let off = 0;
				for (const c of chunks) { all.set(c, off); off += c.byteLength; }
				await this.writeObject(key, all, meta);
				const l = await this.list({ prefix: `__mpu/${uploadId}` });
				await this.delete(l.objects.map((o) => o.key));
				const done = await this.head(key);
				if (!done) throw new Error("Uploaded object not found");
				return done;
			},
		};
	}
}

export function openD1Bucket(db: D1Database): R2Bucket {
	return new D1Bucket(db) as unknown as R2Bucket;
}
