export function waitByPromise(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function generateTimestamp(): number {
  return Date.now();
}
