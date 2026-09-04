export class AgentHubError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "AgentHubError";
    this.code = code;
  }
}

export function asDelegateError(error: unknown): { code: string; message: string } {
  if (error instanceof AgentHubError) {
    return { code: error.code, message: error.message };
  }

  if (error instanceof Error) {
    return { code: "INTERNAL_ERROR", message: error.message };
  }

  return { code: "INTERNAL_ERROR", message: String(error) };
}
