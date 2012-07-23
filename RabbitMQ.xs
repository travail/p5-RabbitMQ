#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"
#include "xshelper.h"

#include <amqp.h>
#include <amqp_framing.h>

typedef struct {
  amqp_connection_state_t conn;
} RabbitMQ;

typedef struct {
  amqp_connection_state_t conn;
  int channel;
} RabbitMQ_Channel;

typedef struct {
} RabbitMQ_Queue;

int rabbitmq_read_frame(HV *RETVAL, amqp_connection_state_t conn, int piggyback)
{
  amqp_frame_t frame;
  amqp_basic_deliver_t *d;
  amqp_basic_properties_t *p;
  size_t body_target;
  size_t body_received;
  int result;

  result = 0;
  while (1) {
    SV *payload;
    HV *props = newHV();

    if (!piggyback) {
      amqp_maybe_release_buffers(conn);
      result = amqp_simple_wait_frame(conn, &frame);
      if (result <= 0) break;
      if (frame.frame_type != AMQP_FRAME_METHOD) continue;
      if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) continue;
      d = (amqp_basic_deliver_t *) frame.payload.method.decoded;
      hv_store(RETVAL, "delivery_tag", strlen("delivery_tag"),
               newSViv(d->delivery_tag), 0);
      hv_store(RETVAL, "exchange", strlen("exchange"),
               newSVpvn((const char *) d->exchange.bytes, d->exchange.len), 0);
      hv_store(RETVAL, "consumer_tag", strlen("consumer_tag"),
               newSVpvn((const char *) d->consumer_tag.bytes, d->consumer_tag.len), 0);
      hv_store(RETVAL, "routing_key", strlen("routing_key"),
               newSVpvn((const char *) d->routing_key.bytes, d->routing_key.len), 0);
      piggyback = 0;
    }

    result = amqp_simple_wait_frame(conn, &frame);
    if (frame.frame_type == AMQP_FRAME_HEARTBEAT) continue;
    if (result < 0) break;
    if (frame.frame_type != AMQP_FRAME_HEADER)
      Perl_croak(aTHX_ "Unexpected header: %d", frame.frame_type);

    hv_store(RETVAL, "props", strlen("props"), newRV_noinc((SV *) props), 0);
    p = (amqp_basic_properties_t *) frame.payload.properties.decoded;
    if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
      hv_store(props, "content_type", strlen("content_type"),
               newSVpvn((const char *) p->content_type.bytes, p->content_type.len), 0);
      
    }
    if (p->_flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) {
      hv_store(props, "content_encoding", strlen("content_encoding"),
               newSVpvn((const char *) p->content_encoding.bytes, p->content_encoding.len), 0);
    }
    if (p->_flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
      hv_store(props, "correlation_id", strlen("correlation_id"),
               newSVpvn((const char *) p->correlation_id.bytes, p->correlation_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_REPLY_TO_FLAG) {
      hv_store(props, "reply_to", strlen("reply_to"),
               newSVpvn((const char *) p->reply_to.bytes, p->reply_to.len), 0);
    }
    if (p->_flags & AMQP_BASIC_EXPIRATION_FLAG) {
      hv_store(props, "expiration", strlen("expiration"),
               newSVpvn((const char *) p->expiration.bytes, p->expiration.len), 0);
    }
    if (p->_flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
      hv_store(props, "message_id", strlen("message_id"),
               newSVpvn((const char *) p->message_id.bytes, p->message_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_TYPE_FLAG) {
      hv_store(props, "type", strlen("type"),
               newSVpvn((const char *) p->type.bytes, p->type.len), 0);
    }
    if (p->_flags & AMQP_BASIC_USER_ID_FLAG) {
      hv_store(props, "user_id", strlen("user_id"),
               newSVpvn((const char *) p->user_id.bytes, p->user_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_APP_ID_FLAG) {
      hv_store(props, "app_id", strlen("app_id"),
               newSVpvn((const char*) p->app_id.bytes, p->app_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_DELIVERY_MODE_FLAG) {
      hv_store(props, "delivery_mode", strlen("delivery_mode"),
               newSViv(p->delivery_mode), 0);
    }
    if (p->_flags & AMQP_BASIC_PRIORITY_FLAG) {
      hv_store(props, "priority", strlen("priority"),
               newSViv(p->priority), 0);
    }
    if (p->_flags & AMQP_BASIC_TIMESTAMP_FLAG) {
      hv_store(props, "timestamp", strlen("timestamp"),
               newSViv(p->timestamp), 0);
    }
    if (p->_flags & AMQP_BASIC_HEADERS_FLAG) {
      int i;
      SV *val;
      HV *headers = newHV();
      hv_store(props, "headers", strlen("headers"), newRV_noinc((SV *) headers), 0);

      for (i = 0; i < p->headers.num_entries; i++) {
      }
    }

    body_target = frame.payload.properties.body_size;
    body_received = 0;
    payload = newSVpvn("", 0);
    while (body_received < body_target) {
      result = amqp_simple_wait_frame(conn, &frame);
      if (result < 0) break;
      if (frame.frame_type != AMQP_FRAME_BODY)
        Perl_croak(aTHX_ "Expected body got: %d", frame.frame_type);

      body_received += frame.payload.body_fragment.len;
      assert(body_received <= body_target);
      sv_catpvn(payload, (const char *) frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
    }

    if (body_received != body_target) {
      Perl_croak(aTHX_ "");
    }

    hv_store(RETVAL, "body", strlen("body"), payload, 0);
    break;
  }

  return result;
}

MODULE = RabbitMQ    PACKAGE = RabbitMQ::Constants

INCLUDE: const-xs.inc

MODULE = RabbitMQ PACKAGE = RabbitMQ PREFIX = rabbitmq_

PROTOTYPES:     DISABLE

RabbitMQ *
rabbitmq_xs_new()
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
  SV   **svp;
  STRLEN len;
  amqp_rpc_reply_t rpc_reply;
CODE:
{
  if ((svp = hv_fetch(args, "host", 4, 0)) != NULL && SvPOK(*svp))
    host = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "port", 4, 0)) != NULL && SvIOK(*svp))
    port = SvIV(*svp);
  if ((svp = hv_fetch(args, "user", 4, 0)) != NULL && SvPOK(*svp))
    user = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "password", 8, 0)) != NULL && SvPOK(*svp))
    password = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "vhost", 5, 0)) != NULL && SvPOK(*svp))
    vhost = SvPV(*svp, len);
  if ((svp = hv_fetch(args, "max_channel", 11, 0)) != NULL && SvIOK(*svp))
    max_channel = SvIV(*svp);
  if ((svp = hv_fetch(args, "max_frame", 9, 0)) != NULL && SvIOK(*svp))
    max_frame = SvIV(*svp);
  if ((svp = hv_fetch(args, "heartbeat", 9, 0)) != NULL && SvIOK(*svp))
    heartbeat = SvIV(*svp);

  sockfd = amqp_open_socket(host, port);
  if (sockfd < 0)
    Perl_croak(aTHX_ "Cannot open socket");
  amqp_set_sockfd(mq->conn, sockfd);
  rpc_reply = amqp_login(mq->conn, vhost, max_channel, max_frame,
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
PREINIT:
  amqp_rpc_reply_t rpc_reply;
CODE:
{
  rpc_reply = amqp_connection_close(mq->conn, AMQP_REPLY_SUCCESS);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot disconnect");

  RETVAL = amqp_destroy_connection(mq->conn);
  free(mq);
}
OUTPUT:
  RETVAL

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
    Newxz(ch, sizeof(ch), RabbitMQ_Channel);
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

  RETVAL = newHV();
  sv_2mortal((SV *) RETVAL);
  hv_store(RETVAL, "queue", 5, newSVpvn((const char *) queue_declare_ok->queue.bytes, queue_declare_ok->queue.len), 0);
  hv_store(RETVAL, "message_count", 13, newSVuv(queue_declare_ok->message_count), 0);
  hv_store(RETVAL, "consumer_count", 14, newSVuv(queue_declare_ok->consumer_count), 0);
}
OUTPUT:
  RETVAL

HV *
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

  RETVAL = newHV();
  sv_2mortal((SV *) RETVAL);
  hv_store(RETVAL, "queue", 5, newSVpvn(queue, strlen(queue)), 0);
  hv_store(RETVAL, "message_count", 13, newSVuv(queue_delete_ok->message_count), 0);
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
/* These options below are not supported by amqp_exchange_declare()
  amqp_boolean_t auto_delete = 0;
  amqp_boolean_t internal    = 0;
  amqp_boolean_t nowait      = 0;
*/
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
  char  *exchange = "amq.direct";
  char  *routing_key;
  char  *body;
  amqp_bytes_t exchange_b;
  amqp_bytes_t routing_key_b;
  amqp_bytes_t body_b;
  HV    *props = NULL;
  amqp_boolean_t mandatory = FALSE;
  amqp_boolean_t immediate = FALSE;
  SV   **svp_props;
  SV   **svp;
CODE:
{
  if ((svp = hv_fetch(args, "exchange", strlen("exchange"), 0)) != NULL && SvPOK(*svp)) {
    exchange = SvPV(*svp, len);
    exchange_b.bytes = exchange;
    exchange_b.len   = len;
  }
  if ((svp = hv_fetch(args, "routing_key", strlen("routing_key"), 0)) != NULL && SvPOK(*svp)) {
    routing_key = SvPV(*svp, len);
    routing_key_b.bytes = routing_key;
    routing_key_b.len   = len;
  }
  if ((svp = hv_fetch(args, "body", strlen("body"), 0)) != NULL) {
    body = SvPV(*svp, len);
    body_b.bytes = body;
    body_b.len   = len;
  }
  if ((svp = hv_fetch(args, "mandatory", strlen("mandatory"), 0)) != NULL && SvIOK(*svp)) {
    mandatory = SvIV(*svp) == 0 ? FALSE : TRUE;
  }
  if ((svp = hv_fetch(args, "immediate", strlen("immediate"), 0)) != NULL && SvIOK(*svp)) {
    immediate = SvTRUE(*svp);
  }

  if (hv_exists(args, "props", strlen("props"))) {
    svp_props = hv_fetch(args, "props", strlen("props"), 0);
    if (SvROK(*svp_props) && SvTYPE(SvRV(*svp_props)) == SVt_PVHV)
      props = (HV *) SvRV(*svp_props);
  }

  properties.headers = AMQP_EMPTY_TABLE;
  properties._flags  = 0;
  if (props) {
    if ((svp_props = hv_fetch(props, "content_type", strlen("content_type"), 0)) != NULL && SvPOK(*svp_props)) {
      properties.content_type = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
    }
    if ((svp_props = hv_fetch(props, "content_encoding", strlen("content_encoding"), 0)) != NULL && SvPOK(*svp_props)) {
      properties.content_encoding = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
    }
    if ((svp_props = hv_fetch(props, "correlation_id", strlen("correlation_id"), 0)) != NULL && (SvPOK(*svp_props)|| SvIOK(*svp_props))) {
      properties.correlation_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "reply_to", strlen("reply_to"), 0)) != NULL && (SvPOK(*svp_props) ||SvIOK(*svp_props))) {
      properties.reply_to = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
    }
    if ((svp_props = hv_fetch(props, "expiration", strlen("expiration"), 0)) != NULL && (SvPOK(*svp_props) || SvIOK(*svp_props))) {
      properties.expiration = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_EXPIRATION_FLAG;
    }
    if ((svp_props = hv_fetch(props, "message_id", strlen("message_id"), 0)) != NULL && (SvPOK(*svp_props) || SvIOK(*svp_props))) {
      properties.message_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "type", strlen("type"), 0)) != NULL && SvPOK(*svp_props)) {
      properties.type = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_TYPE_FLAG;
    }
    if ((svp_props = hv_fetch(props, "user_id", strlen("user_id"), 0)) != NULL && SvPOK(*svp_props)) {
      properties.user_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_USER_ID_FLAG;
    }
    if ((svp_props = hv_fetch(props, "app_id", strlen("app_id"), 0)) != NULL && (SvPOK(*svp_props) || SvIOK(*svp_props))) {
      properties.app_id = amqp_cstring_bytes(SvPV(*svp_props, len));
      properties._flags |= AMQP_BASIC_APP_ID_FLAG;
    }
    /* delivery_mode must be 1 or 2 */
    if ((svp_props = hv_fetch(props, "delivery_mode", strlen("delivery_mode"), 0)) != NULL && SvIOK(*svp_props)) {
      properties.delivery_mode = (uint8_t) SvIV(*svp_props);
      properties._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
    }
    /* priority must be 0 to 9 */
    if ((svp_props = hv_fetch(props, "priority", strlen("priority"), 0)) != NULL && SvIOK(*svp_props)) {
      properties.priority = (uint8_t) SvIV(*svp_props);
      properties._flags |= AMQP_BASIC_PRIORITY_FLAG;
    }
    if ((svp_props = hv_fetch(props, "timestamp", strlen("timestamp"), 0)) != NULL && SvIOK(*svp_props)) {
      properties.timestamp = (uint64_t) SvIV(*svp_props);
      properties._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
    }
  }

  /*
    librabbitmq does not raise a channel exception if the exchange dose not exist.
  */
  RETVAL = amqp_basic_publish(ch->conn, ch->channel, exchange_b, routing_key_b,
                              mandatory, immediate, &properties, body_b);
}
OUTPUT:
  RETVAL

SV *
rabbitmq_basic_consume(ch, queue, opts)
  RabbitMQ_Channel *ch
  char *queue
  HV *opts
PREINIT:
  amqp_rpc_reply_t rpc_reply;
  amqp_basic_consume_ok_t *basic_consume_ok;
  amqp_bytes_t   queue_b        = AMQP_EMPTY_BYTES;
  amqp_bytes_t   consumer_tag_b = AMQP_EMPTY_BYTES;
  amqp_boolean_t no_local  = 0;
  amqp_boolean_t no_ack    = 0;
  amqp_boolean_t exclusive = 0;
  amqp_table_t   args      = AMQP_EMPTY_TABLE;
  SV **svp;
  STRLEN len;
CODE:
{
  if (queue && strcmp(queue, ""))
    queue_b = amqp_cstring_bytes(queue);
  if ((svp = hv_fetch(opts, "consumer_tag", 12, 0)) != NULL && SvPOK(*svp))
    consumer_tag_b = amqp_cstring_bytes(SvPV(*svp, len));
  if ((svp = hv_fetch(opts, "no_local", 8, 0)) != NULL && SvIOK(*svp))
    no_local = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "no_ack", 6, 0)) != NULL && SvIOK(*svp))
    no_ack = (amqp_boolean_t) SvIV(*svp);
  if ((svp = hv_fetch(opts, "exclusive", 9, 0)) != NULL && SvIOK(*svp))
    exclusive = (amqp_boolean_t) SvIV(*svp);

  basic_consume_ok = amqp_basic_consume(ch->conn, ch->channel, queue_b, consumer_tag_b,
                                        no_local, no_ack, exclusive, args);
  rpc_reply = amqp_get_rpc_reply(ch->conn);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Cannot consume messages from %s", queue);

  RETVAL = newSVpvn((char *) basic_consume_ok->consumer_tag.bytes,
                    (int) basic_consume_ok->consumer_tag.len);
}
OUTPUT:
  RETVAL

SV *
rabbitmq_basic_get(ch, queue, opts)
  RabbitMQ_Channel *ch
  char *queue
  HV *opts
PREINIT:
  amqp_rpc_reply_t rpc_reply;
  amqp_bytes_t queue_b = AMQP_EMPTY_BYTES;
  amqp_boolean_t no_ack = TRUE;
  SV **svp;
CODE:
{
  if (queue && strcmp(queue, "")) {
    queue_b = amqp_cstring_bytes(queue);
  }
  if ((svp = hv_fetch(opts, "no_ack", 6, 0)) != NULL && SvIOK(*svp)) {
    no_ack = SvTRUE(*svp);
  }
  amqp_maybe_release_buffers(ch->conn);
  rpc_reply = amqp_basic_get(ch->conn, ch->channel, queue_b, no_ack);
  if (rpc_reply.reply_type != AMQP_RESPONSE_NORMAL)
    Perl_croak(aTHX_ "Could not get a message from %s", queue);

  if (rpc_reply.reply.id == AMQP_BASIC_GET_OK_METHOD) {
    HV *res;
    amqp_basic_get_ok_t *ok = (amqp_basic_get_ok_t *) rpc_reply.reply.decoded;
    res = newHV();
    hv_store(res, "delivery_tag", strlen("delivery_tag"),
             newSViv(ok->delivery_tag), 0);
    hv_store(res, "redelivered", strlen("redelivered"),
             newSViv(ok->redelivered), 0);
    hv_store(res, "exchange", strlen("exchange"),
             newSVpvn((const char *) ok->exchange.bytes, ok->exchange.len), 0);
    hv_store(res, "routing_key", strlen("routing_key"),
             newSVpvn((const char *) ok->routing_key.bytes, ok->routing_key.len), 0);
    hv_store(res, "message_count", strlen("message_count"),
             newSViv(ok->message_count), 0);

    if (amqp_data_in_buffer(ch->conn)) {
      int rv;
      rv = rabbitmq_read_frame(res, ch->conn, 1);
      if (rv < 0)
        Perl_croak(aTHX_ "Could not read frame");
    }
    RETVAL = (SV *) newRV_noinc((SV *) res);
  }
  else {
    // TODO: warn amqp_method_name(rpc_reply.reply.id) or something
    RETVAL = &PL_sv_undef;
  }
}
OUTPUT:
  RETVAL

int
rabbitmq_basic_ack(ch, delivery_tag, multiple)
  RabbitMQ_Channel *ch
  SV *delivery_tag
  SV *multiple
PREINIT:
  int amqp_delivery_tag = 0;
  amqp_boolean_t amqp_multiple = FALSE;
CODE:
{
  if (delivery_tag && SvIOK(delivery_tag)) {
    amqp_delivery_tag = SvIV(delivery_tag);
  }
  else {
    croak("precondition-failed %d: delivery_tag should be non-zero integer", AMQP_PRECONDITION_FAILED);
  }
  if (multiple && SvIOK(multiple))
    amqp_multiple = (amqp_boolean_t) SvTRUE(multiple);
  
  RETVAL = amqp_basic_ack(ch->conn, ch->channel, amqp_delivery_tag, amqp_multiple);
}
OUTPUT:
  RETVAL
