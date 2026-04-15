
interface ApiSessionSubmitResponse {
  id?: string;
  intent?: string;
  queued?: boolean;
  reservedStatus?: string;
  additionalProperties?: Map<string, any>;
}
export { ApiSessionSubmitResponse };