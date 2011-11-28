#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"
#include "amqp.h"
#include "amqp_framing.h"

typedef amqp_connection_state_t  RabbitMQ;

MODULE = RabbitMQ PACKAGE = RabbitMQ PREFIX = rabbitmq_

PROTOTYPES:     DISABLE

#define int_from_hv(hv,name) \
  do { SV **v; if(NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvIV(*v); } while(0)
#define double_from_hv(hv,name) \
 do { SV **v; if(NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvNV(*v); } while(0)
#define str_from_hv(hv,name) \
  do { SV **v; if(NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvPV_nolen(*v); } while(0)

RabbitMQ
rabbitmq_new(char *class)
  CODE:
    RETVAL = amqp_new_connection();
  OUTPUT:
    RETVAL

int
rabbitmq_connect(conn, args)
    RabbitMQ conn
    HV *args
  PREINIT:
    int   sockfd;
    char  *host        = "localhost";
    int    port        = 5672;
    char  *user        = "guest";
    char  *password    = "guest";
    char  *vhost       = "/";
    int    max_channel = 0;
    int    max_frame   = 131072;
    int    heartbeat   = 0;
    double timeout     = -1;
    struct timeval to;
  CODE:
    str_from_hv(args, host);
    int_from_hv(args, port);
    str_from_hv(args, user);
    str_from_hv(args, password);
    str_from_hv(args, vhost);
    int_from_hv(args, max_channel);
    int_from_hv(args, max_frame);
    int_from_hv(args, heartbeat);
    double_from_hv(args, timeout);

    if (timeout >= 0) {
      to.tv_sec  = floor(timeout);
      to.tv_usec = 1000000.0 * (timeout - floor(timeout));
    }

    sockfd = amqp_open_socket(host, port);
    if (sockfd < 0)
      Perl_croak(aTHX_ "Cannot connect to rabbitmq-server");

    amqp_set_sockfd(conn, sockfd);
    amqp_rpc_reply_t res;
    res = amqp_login(conn, vhost, max_channel, max_frame, heartbeat,
                     AMQP_SASL_METHOD_PLAIN, user, password);
    if (res.reply_type != AMQP_RESPONSE_NORMAL)
      Perl_croak(aTHX_ "Connot login to rabbitmq-server");

    RETVAL = sockfd;
  OUTPUT:
    RETVAL

void
rabbitmq_disconnect(conn)
    RabbitMQ conn
  PREINIT:
    int sockdf;
  CODE:
    amqp_rpc_reply_t res;
    res = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    if (res.reply_type != AMQP_RESPONSE_NORMAL) {
      Perl_croak(aTHX_ "Cannot disconnect");
    }

    sockdf = amqp_get_sockfd(conn);
    if (sockdf >= 0) close(sockdf);
    amqp_set_sockfd(conn, -1);
