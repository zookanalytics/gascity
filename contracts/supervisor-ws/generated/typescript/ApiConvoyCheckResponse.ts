
interface ApiConvoyCheckResponse {
  reservedClosed?: number;
  complete?: boolean;
  convoyId?: string;
  total?: number;
  additionalProperties?: Map<string, any>;
}
export { ApiConvoyCheckResponse };