export function integer2LexString(number: number): string {
  let result = ""+number;
  while (result.length < 14) {
    result = "0"+result;
  }
  return result;
}