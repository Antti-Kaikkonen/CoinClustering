export function JSONtoAmount(value: number): number {
  return Math.round(1e8 * value);
}