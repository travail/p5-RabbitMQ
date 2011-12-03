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
rabbitmq_xs_new(class, ...)
    char *class;
  PREINIT:
    RabbitMQ *mq;
  CODE:
    mq = rabbitmq_xs_new(class, ST(1));
    RETVAL = mq;
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
    amqp_rpc_reply_t res;
  CODE:
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
    res = rabbitmq_login(mq->conn, vhost, max_channel, max_frame,
                         heartbeat, AMQP_SASL_METHOD_PLAIN, user, password);
    if (res.reply_type != AMQP_RESPONSE_NORMAL)
      Perl_croak(aTHX_ "Connot login to rabbitmq-server");

    RETVAL = res.reply_type;
  OUTPUT:
    RETVAL

void
rabbitmq_disconnect(mq)
  RabbitMQ *mq;
