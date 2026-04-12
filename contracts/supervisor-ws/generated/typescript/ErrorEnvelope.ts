import {AnonymousSchema_58} from './AnonymousSchema_58';
import {FieldError} from './FieldError';
interface ErrorEnvelope {
  reservedType: AnonymousSchema_58;
  id?: string;
  code: string;
  message: string;
  details?: FieldError[];
}
export { ErrorEnvelope };