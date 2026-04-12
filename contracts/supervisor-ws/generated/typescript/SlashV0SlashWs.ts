import {HelloEnvelope} from './HelloEnvelope';
import {ResponseEnvelope} from './ResponseEnvelope';
import {ErrorEnvelope} from './ErrorEnvelope';
import {EventEnvelope} from './EventEnvelope';
type SlashV0SlashWs = HelloEnvelope | ResponseEnvelope | ErrorEnvelope | EventEnvelope;
export { SlashV0SlashWs };