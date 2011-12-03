#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"
#include <amqp.h>
#include <amqp_framing.h>

typedef struct {
  amqp_connection_state_t conn;
} RabbitMQ;

typedef struct {
} RabbitMQ_Channel;

typedef struct {
} RabbitMQ_Queue;

//RabbitMQ *rabbitmq_new(char *class);
RabbitMQ *rabbitmq_xs_new(char *class, ...);
int rabbitmq_connect(RabbitMQ *self, HV* args);
void rabbitmq_disconnect(RabbitMQ *self);
int rabbitmq_open_socket(char *host, int port);
void rabbitmq_set_sockfd(amqp_connection_state_t conn, int sockfd);
amqp_rpc_reply_t rabbitmq_login(amqp_connection_state_t conn, char *vhost, int max_channel, int max_frame,
                                int heartbeat, amqp_sasl_method_enum sasl_method, char *user, char *password);
