import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { InteractionKernel } from "../src/kernel/index.js";
import type { TransportFactory } from "../src/kernel/index.js";
import { FakeFactory, FakeProviderFactory, fixedClock, fullCapabilities } from "./kernel-fakes.js";

async function expectCode(run: () => Promise<unknown>, code: string): Promise<AgentHubError> {
  try {
    await run();
  } catch (error) {
    expect(error).toBeInstanceOf(AgentHubError);
    expect((error as AgentHubError).code).toBe(code);
    return error as AgentHubError;
  }
  throw new Error(`expected ${code} to be thrown, nothing was`);
}

describe("transport selection", () => {
  it("refuses when no injected factory pairs with the provider", async () => {
    const kernel = new InteractionKernel({
      transportFactories: [new FakeFactory("omp-rpc", "omp")],
      ...fixedClock,
    });
    const error = await expectCode(
      () => kernel.selectTransport("unknown-provider"),
      "TRANSPORT_UNAVAILABLE",
    );
    expect(error.message).toContain("pairs with provider");
  });

  it("obeys an honest not-found probe instead of guessing (OMP RPC v2 bar)", async () => {
    const omp = new FakeFactory("omp-rpc", "omp");
    omp.probeResult = {
      found: false,
      version: "17.0.1",
      detail: "RPC v2 dialect evidence missing; installed command cannot serve the v2 handshake",
    };
    const kernel = new InteractionKernel({ transportFactories: [omp], ...fixedClock });
    const error = await expectCode(
      () => kernel.start({ provider: "omp", workspace: "/ws" }),
      "TRANSPORT_UNAVAILABLE",
    );
    expect(error.message).toContain("RPC v2 dialect evidence missing");
    // Nothing may be created or launched once honesty says "not found".
    expect(omp.created.length).toBe(0);
  });

  it("honors a provider factory's honest decline without fallback", async () => {
    const preferred = new FakeFactory("omp-rpc", "omp");
    const backup = new FakeFactory("omp-legacy", "omp");
    const providerFactory = new FakeProviderFactory("omp", ["omp-rpc", "omp-legacy"]);
    providerFactory.pick = () => null;
    const kernel = new InteractionKernel({
      transportFactories: [preferred, backup],
      providerFactories: [providerFactory],
      ...fixedClock,
    });
    const error = await expectCode(
      () => kernel.start({ provider: "omp", workspace: "/ws" }),
      "TRANSPORT_UNAVAILABLE",
    );
    expect(error.message).toContain("honestly declined");
    expect(preferred.created.length).toBe(0);
    expect(backup.created.length).toBe(0);
  });

  it("lets the provider factory pick by preference, not registration order", async () => {
    const alpha = new FakeFactory("alpha", "fake");
    const beta = new FakeFactory("beta", "fake");
    const providerFactory = new FakeProviderFactory("fake", ["beta", "alpha"]);
    providerFactory.pick = (factories) =>
      [...factories].sort(
        (a, b) =>
          providerFactory.transports.indexOf(a.transport) -
          providerFactory.transports.indexOf(b.transport),
      )[0] ?? null;
    const kernel = new InteractionKernel({
      transportFactories: [alpha, beta],
      providerFactories: [providerFactory],
      ...fixedClock,
    });
    const selection = await kernel.selectTransport("fake");
    expect(selection.factory).toBe(beta);
    const result = await kernel.start({ provider: "fake", workspace: "/ws" });
    expect(result.record.transport).toBe("beta");
    expect(beta.created.length).toBe(1);
    expect(alpha.created.length).toBe(0);
  });

  it("pins to an explicit transport and refuses pins that do not pair", async () => {
    const alpha = new FakeFactory("alpha", "fake");
    const beta = new FakeFactory("beta", "fake");
    const kernel = new InteractionKernel({ transportFactories: [alpha, beta], ...fixedClock });
    const result = await kernel.start({ provider: "fake", transport: "beta", workspace: "/ws" });
    expect(result.record.transport).toBe("beta");
    expect(alpha.created.length).toBe(0);
    await expectCode(
      () => kernel.start({ provider: "fake", transport: "gamma", workspace: "/ws" }),
      "TRANSPORT_UNAVAILABLE",
    );
  });

  it("attaches the probe document to a successful selection", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({ transportFactories: [factory], ...fixedClock });
    const selection = await kernel.selectTransport("fake");
    expect(selection.probe).toEqual({
      transport: "fake-rpc",
      provider: "fake",
      found: true,
      version: "9.9.9",
      detail: "fake installed",
    });
  });

  it("refuses a transport that describes a foreign identity", async () => {
    // A factory whose instances describe themselves as a different
    // transport than the factory advertises: launch must not register it.
    const foreign: TransportFactory = {
      transport: "fake-rpc",
      provider: "fake",
      probe: async () => ({ found: true, version: null, detail: null }),
      create: () => ({
        id: "other-rpc",
        provider: "fake",
        describe: async () => ({
          transport: "other-rpc",
          provider: "fake",
          capabilities: fullCapabilities(),
        }),
        open: async () => ({ pid: null, provider_session_id: null, launched_at: "2026-09-07T00:00:00.000Z" }),
        send: async () => undefined,
        events: () => ({ [Symbol.asyncIterator]: () => ({ next: async () => ({ value: undefined as never, done: true as const }) }) }),
        stop: async () => ({ status: "closed", exit_code: 0, exit_signal: null, waited_ms: 0 }),
      }),
    };
    const kernel = new InteractionKernel({ transportFactories: [foreign], ...fixedClock });
    const error = await expectCode(
      () => kernel.start({ provider: "fake", workspace: "/ws" }),
      "TRANSPORT_PAIRING_INVALID",
    );
    expect(error.message).toContain("other-rpc");
  });

  it("enforces the live-session quota before launching anything", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      maxLiveSessions: 1,
      ...fixedClock,
    });
    await kernel.start({ provider: "fake", workspace: "/ws" });
    await expectCode(
      () => kernel.start({ provider: "fake", workspace: "/ws" }),
      "SESSION_QUOTA_FULL",
    );
    expect(factory.created.length).toBe(1);
  });

  it("refuses an empty workspace (no implicit caller-cwd launch)", async () => {
    const kernel = new InteractionKernel({
      transportFactories: [new FakeFactory("fake-rpc", "fake")],
      ...fixedClock,
    });
    await expectCode(
      () => kernel.start({ provider: "fake", workspace: "" }),
      "COMMAND_INVALID",
    );
  });
});
