import { session } from "../auth/memorySession";
import type { DestinationListDto } from "./dto";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export class ApiClient {
  async getDestinations(signal?: AbortSignal): Promise<DestinationListDto> {
    const request = session.beginRequest(signal);
    try {
      const response = await fetch("/v1/destinations", {
        headers: { Authorization: `Bearer ${request.token}` },
        signal: request.signal,
      });
      if (request.signal.aborted) {
        throw new DOMException("The request was aborted.", "AbortError");
      }
      if (response.status === 401) {
        session.logout("Your API token was rejected. Enter a valid token to continue.");
        throw new ApiError("Your API token was rejected.", 401);
      }
      if (!response.ok) {
        throw new ApiError("The API could not complete the request.", response.status);
      }

      let result: unknown;
      try {
        result = await response.json();
      } catch {
        throw new ApiError("The API returned an invalid response.", response.status);
      }
      if (request.signal.aborted) {
        throw new DOMException("The request was aborted.", "AbortError");
      }
      if (!isDestinationList(result)) {
        throw new ApiError("The API returned an invalid response.", response.status);
      }
      return result;
    } catch (error) {
      if (error instanceof ApiError) throw error;
      if (request.signal.aborted || isAbortError(error)) throw error;
      throw new ApiError("The API is unavailable. Check the local server and try again.", null);
    } finally {
      request.release();
    }
  }
}

function isDestinationList(value: unknown): value is DestinationListDto {
  if (typeof value !== "object" || value === null || !("items" in value)) return false;
  return (
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        typeof item === "object" &&
        item !== null &&
        "id" in item &&
        typeof item.id === "string" &&
        "code" in item &&
        typeof item.code === "string",
    )
  );
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export const apiClient = new ApiClient();
