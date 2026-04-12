import {AnonymousSchema_48} from './AnonymousSchema_48';
import {AnonymousSchema_49} from './AnonymousSchema_49';
import {AnonymousSchema_50} from './AnonymousSchema_50';
interface HelloEnvelope {
  reservedType: AnonymousSchema_48;
  protocol: AnonymousSchema_49;
  serverRole: AnonymousSchema_50;
  readOnly: boolean;
  capabilities: string[];
}
export { HelloEnvelope };