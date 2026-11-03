import { getObjectStorageConfig } from "@/lib/storage/b2-config";
import { openB2Bucket } from "@/lib/storage/b2-bucket";
import { openD1Bucket } from "@/lib/storage/d1-bucket";

const wrapped = new WeakMap<object, CloudflareEnv>();
const proxies = new WeakSet<object>();

/**
 * Returns env with BUCKET served by Backblaze B2 (B2_*) or AWS S3 (S3_*) when configured; otherwise env unchanged
 * (the R2 binding on Workers, the file bucket when self-hosted). Every caller reads `env.BUCKET`, so this one
 * place decides where objects live. Proxied rather than spread because Worker bindings are not enumerable.
 */
export function withStorage(env: CloudflareEnv): CloudflareEnv {
	if (proxies.has(env)) return env;
	const cached = wrapped.get(env);
	if (cached) return cached;
	const config = getObjectStorageConfig(env);
	// Tab patch (Nov 3 2026): a BLOB_DB D1 binding serves BUCKET when no R2/S3 is available (tasmiya-mail deploy).
	const blobDb = (env as unknown as { BLOB_DB?: D1Database }).BLOB_DB;
	if (!config && !blobDb) return env;
	const bucket = config ? openB2Bucket(config) : openD1Bucket(blobDb as D1Database);
	const proxy = new Proxy(env, { get: (target, property) => (property === "BUCKET" ? bucket : Reflect.get(target, property)) });
	wrapped.set(env, proxy);
	proxies.add(proxy);
	return proxy;
}
