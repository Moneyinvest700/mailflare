import { eq } from "drizzle-orm";
import { getDb } from "@/db";
import { appSettings, domains } from "@/db/schema";
import { requireAwsConfig } from "@/lib/aws/config";
import { sendSesEmail } from "@/lib/aws/ses";
import { getEmailAddress, parseEmailAddressParts } from "@/lib/email/address";
import type { OutboundProviderConfig, OutboundProviderMessage, ResendKeyStatus } from "@/lib/email/outbound-provider-types";

const RESEND_ENDPOINT = "https://api.resend.com/emails";

/**
 * Each domain picks its own sender: Cloudflare Email Sending, Resend, or none
 * (receive-only). Receiving and storage always stay on Cloudflare. The Resend key
 * is shared by every domain; it comes from the admin setting, falling back to a
 * RESEND_API_KEY secret so it can be provisioned outside the database.
 */
export async function getResendKeyStatus(env: CloudflareEnv): Promise<ResendKeyStatus & { apiKey: string | null }> {
	const [settings] = await getDb(env)
		.select({ resendApiKey: appSettings.resendApiKey })
		.from(appSettings)
		.where(eq(appSettings.id, "default"))
		.limit(1);
	const saved = settings?.resendApiKey?.trim();
	const fromEnv = (env as { RESEND_API_KEY?: string }).RESEND_API_KEY?.trim();
	return {
		configured: !!(saved || fromEnv),
		source: saved ? "settings" : fromEnv ? "environment" : null,
		apiKey: saved || fromEnv || null,
	};
}

export async function getResendApiKey(env: CloudflareEnv): Promise<string> {
	const { apiKey } = await getResendKeyStatus(env);
	if (!apiKey) throw new Error("Resend is selected but no API key is configured");
	return apiKey;
}

/** The provider for the domain a message is sent from. */
export async function getOutboundProviderConfig(env: CloudflareEnv, fromAddress: string): Promise<OutboundProviderConfig> {
	const hostname = getEmailAddress(fromAddress).split("@")[1]?.toLowerCase();
	const [domain] = hostname
		? await getDb(env).select({ provider: domains.sendingProvider }).from(domains).where(eq(domains.hostname, hostname)).limit(1)
		: [];
	if (!domain || domain.provider === "none") {
		throw new Error(`Sending is not set up for ${hostname ?? "this domain"}. Choose a sending provider on the Domains page.`);
	}
	if (domain.provider === "resend") {
		// Tab patch: with Mailjet keys set as worker secrets, the "Resend" choice sends
		// through Mailjet's HTTP API instead (Mailjet accepts free domains; Resend does not).
		const mailjet = getMailjetKeys(env);
		if (mailjet) return { provider: "mailjet", ...mailjet };
		return { provider: "resend", apiKey: await getResendApiKey(env) };
	}
	if (domain.provider === "ses") return { provider: "ses", config: await requireAwsConfig(env) };
	return { provider: "cloudflare" };
}

function toBase64(buffer: ArrayBuffer): string {
	const bytes = new Uint8Array(buffer);
	let binary = "";
	for (let offset = 0; offset < bytes.length; offset += 0x8000) {
		binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
	}
	return btoa(binary);
}

/** Resend does not return the RFC 5322 Message-ID, so we mint one for replies to thread against. */
function mintMessageId(from: string): string {
	const domain = getEmailAddress(from).split("@")[1] || "mailflare.local";
	return `<${crypto.randomUUID()}@${domain}>`;
}

async function sendWithResend(
	apiKey: string,
	message: OutboundProviderMessage,
	idempotencyKey?: string,
): Promise<{ messageId: string }> {
	const messageId = message.headers?.["Message-ID"] ?? mintMessageId(message.from);
	const response = await fetch(RESEND_ENDPOINT, {
		method: "POST",
		headers: {
			Authorization: `Bearer ${apiKey}`,
			"Content-Type": "application/json",
			...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}),
		},
		body: JSON.stringify({
			from: message.from,
			to: message.to,
			...(message.cc?.length ? { cc: message.cc } : {}),
			...(message.bcc?.length ? { bcc: message.bcc } : {}),
			subject: message.subject,
			html: message.html,
			text: message.text,
			headers: { ...message.headers, "Message-ID": messageId },
			attachments: message.attachments?.map((attachment) => ({
				filename: attachment.filename,
				content: toBase64(attachment.content),
				content_type: attachment.type,
				...(attachment.disposition === "inline" && attachment.contentId ? { content_id: attachment.contentId } : {}),
			})),
		}),
	});
	if (!response.ok) {
		const body = (await response.json().catch(() => null)) as { message?: string } | null;
		throw new Error(`Resend: ${body?.message ?? `request failed (${response.status})`}`);
	}
	return { messageId };
}

const MAILJET_ENDPOINT = "https://api.mailjet.com/v3.1/send";
// Headers Mailjet sets itself or takes through dedicated fields.
const MAILJET_RESERVED_HEADERS = new Set(["message-id", "date", "from", "to", "cc", "bcc", "subject", "sender", "return-path", "mime-version", "content-type", "content-transfer-encoding"]);

function getMailjetKeys(env: CloudflareEnv): { apiKey: string; secretKey: string } | null {
	const vars = env as { MAILJET_API_KEY?: string; MAILJET_SECRET_KEY?: string };
	const apiKey = vars.MAILJET_API_KEY?.trim();
	const secretKey = vars.MAILJET_SECRET_KEY?.trim();
	return apiKey && secretKey ? { apiKey, secretKey } : null;
}

function toMailjetAddress(value: string): { Email: string; Name?: string } {
	const { address, name } = parseEmailAddressParts(value);
	return name ? { Email: address, Name: name } : { Email: address };
}

/**
 * Mailjet sets its own Message-ID and refuses a custom one, so the ID we mint is
 * carried in References: a reply's References names it, which is what threading
 * looks up.
 */
async function sendWithMailjet(
	keys: { apiKey: string; secretKey: string },
	message: OutboundProviderMessage,
	idempotencyKey?: string,
): Promise<{ messageId: string }> {
	const messageId = message.headers?.["Message-ID"] ?? mintMessageId(message.from);
	const headers: Record<string, string> = {};
	for (const [key, value] of Object.entries(message.headers ?? {})) {
		if (!MAILJET_RESERVED_HEADERS.has(key.toLowerCase())) headers[key] = value;
	}
	const referencesKey = Object.keys(headers).find((key) => key.toLowerCase() === "references");
	if (referencesKey) {
		if (!headers[referencesKey].includes(messageId)) headers[referencesKey] = `${headers[referencesKey]} ${messageId}`;
	} else {
		headers.References = messageId;
	}
	const inline = message.attachments?.filter((attachment) => attachment.disposition === "inline" && attachment.contentId) ?? [];
	const regular = message.attachments?.filter((attachment) => !(attachment.disposition === "inline" && attachment.contentId)) ?? [];
	const response = await fetch(MAILJET_ENDPOINT, {
		method: "POST",
		headers: {
			Authorization: `Basic ${btoa(`${keys.apiKey}:${keys.secretKey}`)}`,
			"Content-Type": "application/json",
		},
		body: JSON.stringify({
			Messages: [{
				From: toMailjetAddress(message.from),
				To: message.to.map(toMailjetAddress),
				...(message.cc?.length ? { Cc: message.cc.map(toMailjetAddress) } : {}),
				...(message.bcc?.length ? { Bcc: message.bcc.map(toMailjetAddress) } : {}),
				Subject: message.subject,
				...(message.text ? { TextPart: message.text } : {}),
				...(message.html ? { HTMLPart: message.html } : {}),
				Headers: headers,
				TrackOpens: "disabled",
				TrackClicks: "disabled",
				...(idempotencyKey ? { CustomID: idempotencyKey.slice(0, 64) } : {}),
				...(regular.length ? { Attachments: regular.map((attachment) => ({ ContentType: attachment.type, Filename: attachment.filename, Base64Content: toBase64(attachment.content) })) } : {}),
				...(inline.length ? { InlinedAttachments: inline.map((attachment) => ({ ContentType: attachment.type, Filename: attachment.filename, ContentID: attachment.contentId, Base64Content: toBase64(attachment.content) })) } : {}),
			}],
		}),
	});
	const body = (await response.json().catch(() => null)) as { Messages?: { Status?: string; Errors?: { ErrorMessage?: string }[] }[]; ErrorMessage?: string } | null;
	const result = body?.Messages?.[0];
	if (!response.ok || result?.Status !== "success") {
		const reason = result?.Errors?.map((error) => error.ErrorMessage).filter(Boolean).join("; ") || body?.ErrorMessage;
		throw new Error(`Mailjet: ${reason ?? `request failed (${response.status})`}`);
	}
	return { messageId };
}

export async function sendThroughProvider(
	env: CloudflareEnv,
	config: OutboundProviderConfig,
	message: OutboundProviderMessage,
	idempotencyKey?: string,
): Promise<{ messageId: string }> {
	if (config.provider === "mailjet") return sendWithMailjet(config, message, idempotencyKey);
	if (config.provider === "resend") return sendWithResend(config.apiKey, message, idempotencyKey);
	if (config.provider === "ses") return sendSesEmail(config.config, message);
	const response = await env.EMAIL.send({
		from: message.from,
		to: message.to,
		...(message.cc?.length ? { cc: message.cc } : {}),
		...(message.bcc?.length ? { bcc: message.bcc } : {}),
		subject: message.subject,
		headers: message.headers && Object.keys(message.headers).length ? message.headers : undefined,
		html: message.html,
		text: message.text,
		attachments: message.attachments?.map((attachment) =>
			attachment.disposition === "inline" && attachment.contentId
				? { filename: attachment.filename, type: attachment.type, content: attachment.content, disposition: "inline" as const, contentId: attachment.contentId }
				: { filename: attachment.filename, type: attachment.type, content: attachment.content, disposition: "attachment" as const },
		),
	});
	return { messageId: response.messageId };
}
