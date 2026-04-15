import {SessionlogPaginationInfo} from './SessionlogPaginationInfo';
import {ApiOutputTurn} from './ApiOutputTurn';
interface ApiSessionTranscriptResult {
  format?: string;
  id?: string;
  messages?: Map<string, any>[];
  pagination?: SessionlogPaginationInfo;
  template?: string;
  turns?: ApiOutputTurn[];
  additionalProperties?: Map<string, any>;
}
export { ApiSessionTranscriptResult };