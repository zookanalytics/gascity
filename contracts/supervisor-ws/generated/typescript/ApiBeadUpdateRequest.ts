
interface ApiBeadUpdateRequest {
  assignee?: string | null;
  description?: string | null;
  id?: string;
  labels?: string[];
  metadata?: Map<string, string>;
  priority?: Map<string, any>;
  removeLabels?: string[];
  reservedStatus?: string | null;
  title?: string | null;
  reservedType?: string | null;
  additionalProperties?: Map<string, any>;
}
export { ApiBeadUpdateRequest };