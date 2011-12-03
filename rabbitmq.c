#include "rabbitmq.h"

RabbitMQ *rabbitmq_xs_new(char *class, ...)
{
  RabbitMQ *mq;
  Newxz(mq, sizeof(mq), RabbitMQ);
  mq->conn = amqp_new_connection();
  return mq;
}

int rabbitmq_open_socket(char *host, int port)
{
  return amqp_open_socket(host ,port);
}

void rabbitmq_set_sockfd(amqp_connection_state_t conn, int sockfd)
{
  amqp_set_sockfd(conn, sockfd);
}

amqp_rpc_reply_t rabbitmq_login(amqp_connection_state_t conn, char *vhost, int max_channel, int max_frame,
                                int heartbeat, amqp_sasl_method_enum sasl_method, char *user, char *password)
{
  return amqp_login(conn, vhost, max_channel, max_frame, heartbeat, sasl_method, user, password);
}

void rabbitmq_disconnect(RabbitMQ *mq)
{
  int sockfd;
  amqp_rpc_reply_t res;
  res = amqp_connection_close(mq->conn, AMQP_REPLY_SUCCESS);
  if (res.reply_type != AMQP_RESPONSE_NORMAL) {
      Perl_croak(aTHX_ "Cannot disconnect");
  }

  sockfd = amqp_get_sockfd(mq->conn);
  if (sockfd >= 0) close(sockfd);

  return amqp_set_sockfd(mq->conn, -1);
}
