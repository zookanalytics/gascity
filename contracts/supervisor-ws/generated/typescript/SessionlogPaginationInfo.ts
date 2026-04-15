
interface SessionlogPaginationInfo {
  hasOlderMessages?: boolean;
  returnedMessageCount?: number;
  totalCompactions?: number;
  totalMessageCount?: number;
  truncatedBeforeMessage?: string;
  additionalProperties?: Map<string, any>;
}
export { SessionlogPaginationInfo };