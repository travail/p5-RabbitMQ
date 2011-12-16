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
  amqp_rpc_reply_t rpc_reply;
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
  rpc_reply = rabbitmq_login(mq->conn, vhost, max_channel, max_frame,
                                  heartbeat, AMQP_SASL_METHOD_PLAIN, user, password);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Connot login to rabbitmq-server");

  RETVAL = rpc_reply.reply_type;
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
  int channel = 0;
  amqp_rpc_reply_t  rpc_reply;
  RabbitMQ_Channel *ch;
CODE:
{
  if (SvIOKp(sv_ch)) channel = SvIV(sv_ch);
  if (channel == 0)
    Perl_croak(aTHX_ "Cannot open a channel");

  if (!amqp_channel_open(mq->conn, channel))
    Perl_croak(aTHX_ "Cannot open a channel");

  rpc_reply = amqp_get_rpc_reply(mq->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    Perl_croak(aTHX_ "Cannot open a channel");
  } else {
    ch = Newxz(ch, sizeof(ch), RabbitMQ_Channel);
    ch->conn    = mq->conn;
    ch->channel = channel;
  }

  RETVAL = ch;
}
OUTPUT:
  RETVAL

int
rabbitmq_channel_close(mq, channel)
  RabbitMQ *mq
  int channel
PREINIT:
  amqp_rpc_reply_t rpc_reply;
CODE:
{
  rpc_reply = amqp_channel_close(mq->conn, channel, AMQP_REPLY_SUCCESS);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot close channel");

  RETVAL = rpc_reply.reply_type;
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
rabbitmq_open(ch, channel)
  RabbitMQ_Channel *ch
  int channel
PREINIT:
  amqp_rpc_reply_t rpc_reply;
CODE:
{
  amqp_channel_open(ch->conn, channel);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot open channel");

  ch->channel = channel;

  RETVAL = ch->channel;
}
OUTPUT:
  RETVAL

int
rabbitmq_close(ch)
  RabbitMQ_Channel *ch
PREINIT:
  amqp_rpc_reply_t rpc_reply;
CODE:
{
  amqp_channel_close(ch->conn, ch->channel, AMQP_REPLY_SUCCESS);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot close channel");

  RETVAL = rpc_reply.reply_type;
}
OUTPUT:
  RETVAL

HV *
rabbitmq_queue_declare(ch, queue, opts = NULL)
  RabbitMQ_Channel *ch
  char *queue
  HV   *opts
PREINIT:
  amqp_rpc_reply_t         rpc_reply;
  amqp_queue_declare_ok_t *queue_declare_ok;
  amqp_boolean_t passive     = 0;
  amqp_boolean_t durable     = 0;
  amqp_boolean_t exclusive   = 0;
  amqp_boolean_t auto_delete = 1;
  amqp_table_t args    = AMQP_EMPTY_TABLE;
  amqp_bytes_t queue_b = AMQP_EMPTY_BYTES;
  SV **svp;
  HV  *retval;
CODE:
{
  if (queue && strcmp(queue, ""))
    queue_b = amqp_cstring_bytes(queue);
  if ((svp = hv_fetch(opts, "passive", 7, 0)) != NULL && SvIOK(*svp))
    passive = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "durable", 7, 0)) != NULL && SvIOK(*svp))
    durable = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "exclusive", 9, 0)) != NULL && SvIOK(*svp))
    exclusive = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "auto_delete", 11, 1)) != NULL && SvIOK(*svp))
    auto_delete = (amqp_boolean_t) SvIV(*svp);

  queue_declare_ok = amqp_queue_declare(ch->conn, ch->channel, queue_b,
                                        passive, durable,
                                        exclusive, auto_delete, args);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot declare queue: %s", queue);

  retval = newHV();
  (HV *) sv_2mortal((SV *) retval);
  hv_store(retval, "queue", 5, newSVpvn(queue_declare_ok->queue.bytes, queue_declare_ok->queue.len), 0);
  hv_store(retval, "message_count", 13, newSVuv(queue_declare_ok->message_count), 0);
  hv_store(retval, "consumer_count", 14, newSVuv(queue_declare_ok->consumer_count), 0);

  RETVAL = retval;
}
OUTPUT:
  RETVAL

char *
rabbitmq_queue_delete(ch, queue, opts)
  RabbitMQ_Channel *ch
  char *queue
  HV   *opts
PREINIT:
  amqp_rpc_reply_t        rpc_reply;
  amqp_queue_delete_ok_t *queue_delete_ok;
  amqp_boolean_t if_unused = 0;
  amqp_boolean_t if_empty  = 0;
  amqp_boolean_t nowait    = 0;
  amqp_bytes_t   queue_b   = AMQP_EMPTY_BYTES;
  SV **svp;
CODE:
{
  if (queue && strcmp(queue, ""))
    queue_b = amqp_cstring_bytes(queue);
  if ((svp = hv_fetch(opts, "is_unused", 9, 0)) != NULL && SvIOK(*svp))
    if_unused = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "if_empty", 8, 0)) != NULL && SvIOK(*svp))
    if_empty = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "nowait", 6, 0)) != NULL && SvIOK(*svp))
    nowait = SvIV(*svp);

  queue_delete_ok = amqp_queue_delete(ch->conn, ch->channel,
                                      queue_b, if_unused, if_empty);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot delete queue: %s", queue);

  RETVAL = queue;
}
OUTPUT:
  RETVAL

char *
rabbitmq_exchange_declare(ch, exchange, opts)
  RabbitMQ_Channel *ch
  char *exchange
  HV   *opts
PREINIT:
  amqp_rpc_reply_t rpc_reply;
  char *type = "direct";
  amqp_boolean_t passive     = 0;
  amqp_boolean_t durable     = 0;
  amqp_boolean_t auto_delete = 0;
  amqp_boolean_t internal    = 0;
  amqp_boolean_t nowait      = 0;
  amqp_table_t   args        = AMQP_EMPTY_TABLE;
  amqp_bytes_t   exchange_b  = AMQP_EMPTY_BYTES;
  amqp_bytes_t   type_b;
  SV **svp;
  STRLEN len;
CODE:
{
  if (exchange && strcmp(exchange, ""))
    exchange_b = amqp_cstring_bytes(exchange);
  if ((svp = hv_fetch(opts, "type", 4, 0)) != NULL && SvPOK(*svp)) {
    type   = SvPV(*svp, len);
    type_b = amqp_cstring_bytes(type);
  }
  if ((svp = hv_fetch(opts, "passive", 7, 0)) != NULL && SvIOK(*svp))
    passive = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "durable", 7, 0)) != NULL && SvIOK(*svp))
    durable = (amqp_boolean_t) SvIV(*svp);

  amqp_exchange_declare(ch->conn, ch->channel, exchange_b, type_b,
                        passive, durable, args);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot decahre an exchange: %s", exchange);

  RETVAL = exchange;
}
OUTPUT:
  RETVAL

char *
exchange_delete(ch, exchange, opts)
  RabbitMQ_Channel *ch
  char *exchange
  HV   *opts
PREINIT:
  amqp_rpc_reply_t rpc_reply;
  amqp_bytes_t   exchange_b = AMQP_EMPTY_BYTES;
  amqp_boolean_t if_unused  = 1;
  amqp_boolean_t no_wait    = 0;
  SV **svp;
  STRLEN len;
CODE:
{
  if (exchange && strcmp(exchange, ""))
    exchange_b = amqp_cstring_bytes(exchange);
  if ((svp = hv_fetch(opts, "if_unused", 6, 0)) != NULL && SvIOK(*svp))
    if_unused = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "no_wait", 7, 0)) != NULL && SvIOK(*svp))
    no_wait = (amqp_boolean_t) SvIV(*svp);

  amqp_exchange_delete(ch->conn, ch->channel, exchange_b, if_unused);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot delete an exchange: %s", exchange);

  RETVAL = exchange;
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
  char  *exchange = "amq.direct";
  char  *routingkey;
  char  *body;
  amqp_bytes_t exchange_b;
  amqp_bytes_t routingkey_b;
  amqp_bytes_t body_b;
  HV    *props = NULL;
  amqp_boolean_t mandatory = 0;
  amqp_boolean_t immediate = 0;
  SV   **svp_props;
  SV   **svp;
CODE:
{
  if ((svp = hv_fetch(args, "exchange", 8, 0)) != NULL) {
    exchange = SvPV(*svp, len);
    exchange_b.bytes = exchange;
    exchange_b.len   = len;
  }
  if ((svp = hv_fetch(args, "routingkey", 10, 0)) != NULL) {
    routingkey = SvPV(*svp, len);
    routingkey_b.bytes = routingkey;
    routingkey_b.len   = len;
  }
  if ((svp = hv_fetch(args, "body", 4, 0)) != NULL) {
    body = SvPV(*svp, len);
    body_b.bytes = body;
    body_b.len   = len;
  }
  if ((svp = hv_fetch(args, "mandatory", 9, 0)) != NULL)
    mandatory = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(args, "immediate", 9, 0)) != NULL)
    immediate = (amqp_boolean_t) SvIV(*svp);

  if (hv_exists(args, "props", 5)) {
    svp_props = hv_fetch(args, "props", 5, 0);
    if (SvROK(*svp_props) && SvTYPE(SvRV(*svp_props)) == SVt_PVHV)
      props = (HV *) SvRV(*svp_props);
  }

  properties.headers = AMQP_EMPTY_TABLE;
  properties._flags  = 0;
  if (props) {
    if ((svp_props = hv_fetch(props, "content_type", 12, 0)) != NULL) {
      properties.content_type = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
    }
    if ((svp_props = hv_fetch(props, "content_encoding", 16, 0)) != NULL) {
      properties.content_encoding = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
    }
    if ((svp_props = hv_fetch(props, "correlation_id", 14, 0)) != NULL) {
      properties.correlation_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "reply_to", 8, 0)) != NULL) {
      properties.reply_to = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
    }
    if ((svp_props = hv_fetch(props, "expiration", 10, 0)) != NULL) {
      properties.expiration = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_EXPIRATION_FLAG;
    }
    if ((svp_props = hv_fetch(props, "message_id", 10, 0)) != NULL) {
      properties.message_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "type", 4, 0)) != NULL) {
      properties.type = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_TYPE_FLAG;
    }
    if ((svp_props = hv_fetch(props, "user_id", 7, 0)) != NULL) {
      properties.user_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_USER_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "app_id", 6, 0)) != NULL) {
      properties.app_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_APP_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "delivery_mode", 13, 0)) != NULL) {
      properties.delivery_mode = (uint8_t) SvIV(*svp_props);
      properties._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
    }
    if ((svp_props = hv_fetch(props, "prioriry", 8, 0)) != NULL) {
      properties.priority = (uint8_t) SvIV(*svp_props);
      properties._flags |= AMQP_BASIC_PRIORITY_FLAG;
    }
    if ((svp_props = hv_fetch(props, "timestamp", 8, 0)) != NULL) {
      properties.timestamp = (uint64_t) SvIV(*svp_props);
      properties._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
    }
  }

  result = amqp_basic_publish(ch->conn, ch->channel, exchange_b, routingkey_b,
                              (amqp_boolean_t) mandatory, (amqp_boolean_t) immediate,
                              &properties, body_b);
  RETVAL = result;
}
OUTPUT:
  RETVAL
