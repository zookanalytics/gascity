import {SessionlogAgentMapping} from './SessionlogAgentMapping';
interface ApiSessionAgentsResponse {
  agents?: SessionlogAgentMapping[] | null;
  additionalProperties?: Map<string, any>;
}
export { ApiSessionAgentsResponse };