#include "rabbitmq.h"

int rabbitmq_open_socket(char *host, int port)
{
  return amqp_open_socket(host ,port);
}

void rabbitmq_set_sockfd(amqp_connection_state_t conn, int sockfd)
{
  amqp_set_sockfd(conn, sockfd);
}

amqp_rpc_reply_t rabbitmq_login(amqp_connection_state_t conn, char *vhost,
                                int max_channel, int max_frame, int heartbeat,
                                amqp_sasl_method_enum sasl_method,
                                char *user, char *password)
{
  return amqp_login(conn, vhost, max_channel, max_frame, heartbeat, sasl_method, user, password);
}

int rabbitmq_disconnect(RabbitMQ *mq)
{
  int res = 0;
  amqp_rpc_reply_t amqp_rpc_reply;
  amqp_rpc_reply = amqp_connection_close(mq->conn, AMQP_REPLY_SUCCESS);
  if (amqp_rpc_reply.reply_type != AMQP_RESPONSE_NORMAL) {
//    Perl_croak(aTHX_ "Cannot disconnect");
  }
  amqp_destroy_connection(mq->conn);
  return res;
}
