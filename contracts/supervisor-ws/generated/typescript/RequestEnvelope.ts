import {AnonymousSchema_1} from './AnonymousSchema_1';
import {AnonymousSchema_3} from './AnonymousSchema_3';
import {Scope} from './Scope';
import {CityPatchRequest} from './CityPatchRequest';
import {SessionsListRequest} from './SessionsListRequest';
import {BeadsListRequest} from './BeadsListRequest';
import {MailListRequest} from './MailListRequest';
import {MailGetRequest} from './MailGetRequest';
import {MailReplyRequest} from './MailReplyRequest';
import {MailSendRequest} from './MailSendRequest';
import {EventsListRequest} from './EventsListRequest';
import {NameRequest} from './NameRequest';
import {IdRequest} from './IdRequest';
import {ProvidersListRequest} from './ProvidersListRequest';
import {SessionSubmitRequest} from './SessionSubmitRequest';
import {SessionTranscriptRequest} from './SessionTranscriptRequest';
import {SubscriptionStartRequest} from './SubscriptionStartRequest';
import {SubscriptionStopRequest} from './SubscriptionStopRequest';
interface RequestEnvelope {
  reservedType: AnonymousSchema_1;
  id: string;
  action: AnonymousSchema_3;
  idempotencyKey?: string;
  scope?: Scope;
  payload?: CityPatchRequest | SessionsListRequest | BeadsListRequest | MailListRequest | MailGetRequest | MailReplyRequest | MailSendRequest | EventsListRequest | NameRequest | IdRequest | ProvidersListRequest | SessionSubmitRequest | SessionTranscriptRequest | SubscriptionStartRequest | SubscriptionStopRequest;
}
export { RequestEnvelope };