#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include "rabbitmq.h"

#define int_from_hv(hv,name) \
 do { SV **v; if (NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvIV(*v); } while(0)
#define double_from_hv(hv,name) \
 do { SV **v; if (NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvNV(*v); } while(0)
#define str_from_hv(hv,name) \
 do { SV **v; if (NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvPV_nolen(*v); } while(0)

MODULE = RabbitMQ PACKAGE = RabbitMQ PREFIX = rabbitmq_

PROTOTYPES:     DISABLE

RabbitMQ *
rabbitmq_xs_new(class)
  char *class;
PREINIT:
  RabbitMQ *mq;
CODE:
{
  Newxz(mq, sizeof(mq), RabbitMQ);
  mq->conn = amqp_new_connection();
  RETVAL = mq;
}
OUTPUT:
  RETVAL

int
rabbitmq_connect(mq, args)
  RabbitMQ *mq;
  HV* args;
PREINIT:
  int    sockfd;
  char  *host        = "localhost";
  int    port        = 5672;
  char  *user        = "guest";
  char  *password    = "guest";
  char  *vhost       = "/";
  int    max_channel = 0;
  int    max_frame   = 131072;
  int    heartbeat   = 0;
  amqp_rpc_reply_t amqp_rpc_reply;
CODE:
{
  str_from_hv(args, host);
  int_from_hv(args, port);
  str_from_hv(args, user);
  str_from_hv(args, password);
  str_from_hv(args, vhost);
  int_from_hv(args, max_channel);
  int_from_hv(args, max_frame);
  int_from_hv(args, heartbeat);

  sockfd = rabbitmq_open_socket(host, port);
  if (sockfd < 0)
    Perl_croak(aTHX_ "Cannot open socket");
  rabbitmq_set_sockfd(mq->conn, sockfd);
  amqp_rpc_reply = rabbitmq_login(mq->conn, vhost, max_channel, max_frame,
                                  heartbeat, AMQP_SASL_METHOD_PLAIN, user, password);
  if (amqp_rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Connot login to rabbitmq-server");

  RETVAL = amqp_rpc_reply.reply_type;
}
OUTPUT:
  RETVAL

int
rabbitmq_disconnect(mq)
  RabbitMQ *mq;

RabbitMQ_Channel *
rabbitmq_channel_open(mq, sv_ch)
  RabbitMQ *mq
  SV       *sv_ch
PREINIT:
  int ch;
  amqp_rpc_reply_t  amqp_rpc_reply;
  RabbitMQ_Channel *channel;
CODE:
{
  if (SvIOKp(sv_ch)) ch = SvIV(sv_ch);
  else Perl_croak(aTHX_ "channel must be an integer");

  amqp_channel_open(mq->conn, ch);
  amqp_rpc_reply = amqp_get_rpc_reply(mq->conn);
  if (amqp_rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot open channel");

  channel = Newxz(channel, sizeof(channel), RabbitMQ_Channel);
  channel->conn    = mq->conn;
  channel->channel = ch;

  RETVAL = channel;
}
OUTPUT:
  RETVAL

int
rabbitmq_channel_close(mq, channel)
  RabbitMQ *mq
  int channel
PREINIT:
  amqp_rpc_reply_t amqp_rpc_reply;
CODE:
{
  amqp_channel_close(mq->conn, channel, AMQP_REPLY_SUCCESS);
  amqp_rpc_reply = amqp_get_rpc_reply(mq->conn);
  if (amqp_rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot close channel");

  RETVAL = amqp_rpc_reply.reply_type;
}
OUTPUT:
  RETVAL

MODULE = RabbitMQ PACKAGE = RabbitMQ::Channel PREFIX = rabbitmq_

PROTOTYPES:     DISABLE

int
rabbitmq_channel(channel)
  RabbitMQ_Channel *channel
CODE:
{
  RETVAL = channel->channel;
}
OUTPUT:
  RETVAL

int
rabbitmq_open(channel, ch)
  RabbitMQ_Channel *channel
  int ch
PREINIT:
  amqp_rpc_reply_t amqp_rpc_reply;
CODE:
{
  amqp_channel_open(channel->conn, ch);
  amqp_rpc_reply = amqp_get_rpc_reply(channel->conn);
  if (amqp_rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot open channel");

  channel->channel = ch;

  RETVAL = ch;
}
OUTPUT:
  RETVAL

int
rabbitmq_close(channel)
  RabbitMQ_Channel *channel
PREINIT:
  amqp_rpc_reply_t amqp_rpc_reply;
CODE:
{
  amqp_channel_close(channel->conn, channel->channel, AMQP_REPLY_SUCCESS);
  amqp_rpc_reply = amqp_get_rpc_reply(channel->conn);
  if (amqp_rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot close channel");

  RETVAL = amqp_rpc_reply.reply_type;
}
OUTPUT:
  RETVAL

int
rabbitmq_basic_publish(ch, args)
  RabbitMQ_Channel *ch
  HV *args
PREINIT:
  STRLEN len;
  amqp_basic_properties_t properties;
  int    result;
  char  *exchange;
  char  *routingkey;
  char  *body;
  HV    *props;
  amqp_boolean_t mandatory = 0;
  amqp_boolean_t immediate = 0;
  SV   **sv_props;
  SV   **svp;
  char  *content_type;
CODE:
{
  if ((svp = hv_fetch(args, "exchange", 8, 1)) != NULL)
    exchange = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "routingkey", 10, 1)) != NULL)
    routingkey = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "body", 4, 1)) != NULL)
    body = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "mandatory", 9, 0)) != NULL)
    mandatory = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(args, "immediate", 9, 0)) != NULL)
    immediate = (amqp_boolean_t) SvIV(*svp);

  sv_props = hv_fetch(args, "props", 5, 0);
  if (SvROK(*sv_props) && SvTYPE(SvRV(*sv_props)) == SVt_PVHV)
    props = (HV *) SvRV(*sv_props);

  properties.headers = AMQP_EMPTY_TABLE;
  properties._flags  = 0;
  if (props) {
    if ((sv_props = hv_fetch(props, "content_type", 12, 0)) != NULL) {
      properties.content_type = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
    }
    if ((sv_props = hv_fetch(props, "content_encoding", 16, 0)) != NULL) {
      properties.content_encoding = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
    }
    if ((sv_props = hv_fetch(props, "correlation_id", 14, 0)) != NULL) {
      properties.correlation_id = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
    }
    if ((sv_props = hv_fetch(props, "reply_to", 8, 0)) != NULL) {
      properties.reply_to = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
    }
    if ((sv_props = hv_fetch(props, "expiration", 10, 0)) != NULL) {
      properties.expiration = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_EXPIRATION_FLAG;
    }
    if ((sv_props = hv_fetch(props, "message_id", 10, 0)) != NULL) {
      properties.message_id = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
    }
    if ((sv_props = hv_fetch(props, "type", 4, 0)) != NULL) {
      properties.type = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_TYPE_FLAG;
    }
    if ((sv_props = hv_fetch(props, "user_id", 7, 0)) != NULL) {
      properties.user_id = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_USER_ID_FLAG;
    }
    if ((sv_props = hv_fetch(props, "app_id", 6, 0)) != NULL) {
      properties.app_id = amqp_cstring_bytes(SvPV(*sv_props, len));
      properties._flags |= AMQP_BASIC_APP_ID_FLAG;
    }
    if ((sv_props = hv_fetch(props, "delivery_mode", 13, 0)) != NULL) {
      properties.delivery_mode = (uint8_t) SvIV(*sv_props);
      properties._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
    }
    if ((sv_props = hv_fetch(props, "prioriry", 8, 0)) != NULL) {
      properties.priority = (uint8_t) SvIV(*sv_props);
      properties._flags |= AMQP_BASIC_PRIORITY_FLAG;
    }
    if ((sv_props = hv_fetch(props, "timestamp", 8, 0)) != NULL) {
      properties.timestamp = (uint64_t) SvIV(*sv_props);
      properties._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
    }
  }

  result = amqp_basic_publish(ch->conn, ch->channel,
                              amqp_cstring_bytes(exchange), amqp_cstring_bytes(routingkey),
                              mandatory, immediate, &properties, amqp_cstring_bytes(body));
  RETVAL = result;
}
OUTPUT:
  RETVAL
