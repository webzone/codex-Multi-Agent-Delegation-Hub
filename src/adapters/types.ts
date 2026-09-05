import type { AdapterMetadata, AgentAdapter } from "../types.js";

export type AdapterFactory = () => AgentAdapter;

/**
 * Optional capability path for provider-native conversational resume
 * (Package 3). Filesystem continuation is always available for every
 * adapter: the hub materializes a fresh isolated worktree at the session's
 * artifact commit, so a session resumes with all prior file edits regardless
 * of provider support. Native resume only adds the provider's own
 * conversational memory on top of that, and only when the adapter can prove
 * the installed command actually understands the arguments it would emit.
 *
 * An adapter without this capability is filesystem-only — which is the
 * honest status of every built-in adapter until someone verifies the real
 * CLI syntax against an installed binary.
 */

/**
 * Metadata key under which the hub forwards a previously reported provider
 * session id into `AdapterRequest.metadata`. Purely optional: adapters
 * without a verified capability must ignore it.
 */
export const PROVIDER_SESSION_METADATA_KEY = "provider_session_id";

export interface NativeResumeCapability {
  /**
   * Probe the installed command. May resolve true only when the resume
   * syntax has actually been verified (e.g. by inspecting the binary's
   * help output); a missing or throwing probe counts as unverified.
   */
  verify(): Promise<boolean>;
  /**
   * Extra argv to append when resuming provider conversation
   * `providerSessionId`. Consulted only after `verify()` resolved true.
   * The id must be passed as a single argv element — never interpolated
   * into a shell string (all hub execution runs with `shell: false`).
   */
  resumeArguments(providerSessionId: string): string[];
}

export interface NativeResumeCapableAdapter extends AgentAdapter {
  readonly nativeResumeCapability: NativeResumeCapability;
}

/** Structural check; returns null for any adapter without a verified path. */
export function asNativeResumeCapableAdapter(
  adapter: AgentAdapter,
): NativeResumeCapableAdapter | null {
  const candidate = adapter as Partial<NativeResumeCapableAdapter>;
  const capability = candidate.nativeResumeCapability;
  if (
    capability
    && typeof capability.verify === "function"
    && typeof capability.resumeArguments === "function"
  ) {
    return candidate as NativeResumeCapableAdapter;
  }
  return null;
}

/** Extract a usable provider session id from optional request metadata. */
export function readProviderSessionId(
  metadata: AdapterMetadata | null | undefined,
): string | null {
  const value = metadata?.[PROVIDER_SESSION_METADATA_KEY];
  return typeof value === "string" && value.trim() ? value : null;
}
