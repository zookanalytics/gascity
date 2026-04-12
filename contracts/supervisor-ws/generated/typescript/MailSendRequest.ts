
interface MailSendRequest {
  rig?: string;
  reservedFrom?: string;
  to: string;
  subject: string;
  body?: string;
}
export { MailSendRequest };