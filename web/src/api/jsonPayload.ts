// Format the payload's original JSON tokens, without converting numbers through
// JavaScript Number. The caller has already parsed/validated the response JSON.
export function formatResponsePayload(source: string): string {
  const tokens = [...source.matchAll(/"(?:[^"\\]|\\.)*"|[{}[\],:]|[^\s{}[\],:]+/g)];
  let depth = 0;
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index]?.[0];
    if (depth === 1 && token?.startsWith('"') && JSON.parse(token) === "payload" && tokens[index + 1]?.[0] === ":") {
      const start = index + 2;
      const first = tokens[start]?.[0];
      if (first === undefined) throw new Error("Missing payload");
      let end = start;
      if (first === "{" || first === "[") {
        let nested = 0;
        for (; end < tokens.length; end += 1) {
          const value = tokens[end]?.[0];
          if (value === "{" || value === "[") nested += 1;
          if (value === "}" || value === "]") nested -= 1;
          if (nested === 0) break;
        }
      }
      return formatTokens(tokens.slice(start, end + 1).map((entry) => entry[0]));
    }
    if (token === "{" || token === "[") depth += 1;
    if (token === "}" || token === "]") depth -= 1;
  }
  throw new Error("Missing payload");
}

function formatTokens(tokens: string[]): string {
  let result = "";
  let depth = 0;
  const indent = (): string => "  ".repeat(depth);
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "{" || token === "[") {
      result += token;
      const next = tokens[index + 1];
      if (next === "}" || next === "]") {
        result += next;
        index += 1;
      } else {
        depth += 1;
        result += `\n${indent()}`;
      }
    } else if (token === "}" || token === "]") {
      depth -= 1;
      result += `\n${indent()}${token}`;
    } else if (token === ",") {
      result += `,\n${indent()}`;
    } else if (token === ":") {
      result += ": ";
    } else {
      result += token;
    }
  }
  return result;
}
