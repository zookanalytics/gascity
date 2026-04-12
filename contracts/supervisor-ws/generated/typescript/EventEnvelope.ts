import {AnonymousSchema_65} from './AnonymousSchema_65';
interface EventEnvelope {
  reservedType: AnonymousSchema_65;
  subscriptionId: string;
  eventType: string;
  index?: number;
  cursor?: string;
  payload?: Map<string, any>;
}
export { EventEnvelope };