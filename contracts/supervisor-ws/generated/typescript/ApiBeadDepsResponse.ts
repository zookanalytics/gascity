import {BeadsBead} from './BeadsBead';
interface ApiBeadDepsResponse {
  children?: BeadsBead[] | null;
  additionalProperties?: Map<string, any>;
}
export { ApiBeadDepsResponse };