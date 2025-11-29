import { KEY_SIZE_BYTES, VALUE_SIZE_BYTES } from "../constants.ts";

export type KeyInput = Buffer | bigint | number;

export const ZERO_KEY = Buffer.alloc(KEY_SIZE_BYTES, 0);

export function bufferToBigInt(buffer: Buffer): bigint {
  if (buffer.length !== KEY_SIZE_BYTES) {
    throw new Error(`Key buffer must be ${KEY_SIZE_BYTES} bytes`);
  }

  let value = 0n;
  for (const byte of buffer.values()) {
    value = (value << 8n) | BigInt(byte);
  }

  return value;
}

export function bigintToBuffer(key: bigint): Buffer {
  let value = key;
  if (value < 0n) {
    throw new Error("Negative keys are not supported");
  }

  const buffer = Buffer.alloc(KEY_SIZE_BYTES);
  for (let i = KEY_SIZE_BYTES - 1; i >= 0; i -= 1) {
    buffer[i] = Number(value & 0xffn);
    value >>= 8n;
  }

  if (value !== 0n) {
    throw new Error("Key too large to fit in 64 bits");
  }

  return buffer;
}

export function normalizeKeyInput(input: KeyInput): Buffer {
  if (typeof input === "number") {
    if (!Number.isInteger(input) || input < 0) {
      throw new Error("Keys must be positive integers");
    }
    return bigintToBuffer(BigInt(input));
  }

  if (typeof input === "bigint") {
    return bigintToBuffer(input);
  }

  if (input.length !== KEY_SIZE_BYTES) {
    throw new Error(`Keys must be ${KEY_SIZE_BYTES} bytes`);
  }

  return Buffer.from(input);
}

export function normalizeValueInput(value: Buffer): Buffer {
  if (value.length > VALUE_SIZE_BYTES) {
    throw new Error(
      `Values must be <= ${VALUE_SIZE_BYTES} bytes (received ${value.length})`,
    );
  }

  if (value.length === VALUE_SIZE_BYTES) {
    return Buffer.from(value);
  }

  const padded = Buffer.alloc(VALUE_SIZE_BYTES);
  value.copy(padded);
  return padded;
}
