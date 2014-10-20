/* vim:expandtab:shiftwidth=2:tabstop=2:smarttab:
 * Copyright 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain 
 * a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

#include <argp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <libattachsql-1.0/attachsql.h>

const char *argp_program_version= "attachsql 0.1.0";
const char *argp_bug_address= "<andrew@linuxjedi.co.uk>";

static struct argp_option options[]=
{
  {"verbose", 'v', 0, 0, "Verbose output", 0},
  {"connections", 'c', "CONNECTIONS", 0, "Connections per thread", 0},
  {"threads", 't', "THREADS", 0, "Number of threads", 0},
  {"host", 'h', "HOST", 0, "Host name or socket path", 0},
  {"port", 'o', "PORT", 0, "Port number (0 for socket)", 0},
  {"user", 'u', "USER", 0, "Username", 0},
  {"pass", 'p', "PASS", 0, "Password", 0},
  {"db", 'd', "DATABASE", 0, "Database", 0},
  {"queries", 'q', "QUERIES", 0, "Max queries", 0},
  {0}
};

struct arguments
{
  bool verbose;
  uint16_t connections;
  uint16_t threads;
  char *host;
  uint16_t port;
  char *user;
  char *pass;
  char *db;
  uint64_t max_queries;
};

struct connect_data
{
  uint16_t con_no;
  char query[40];
  attachsql_connect_t *con;
  struct arguments *arguments;
  uint64_t *query_counter;
  uint64_t *query_done_counter;
  uint64_t *max_queries;
};

struct connect_thread
{
  uint64_t max_queries;
  pthread_t thread_id;
  uint16_t thread_num;
  struct arguments *arguments;
};

static error_t parse_opt(int key, char *arg, struct argp_state *state);
void callbk(attachsql_connect_t *con, attachsql_events_t events, void *context, attachsql_error_t *error);
void *thread_start(void *arg);

static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
  struct arguments *arguments= state->input;
  switch (key)
  {
    case 'v':
      arguments->verbose= true;
      break;
    case 'c':
      arguments->connections= strtoul(arg, NULL, 10);
      break;
    case 't':
      arguments->threads= strtoul(arg, NULL, 10);
      break;
    case 'h':
      arguments->host= arg;
      break;
    case 'o':
      arguments->port= strtoul(arg, NULL, 10);
      break;
    case 'u':
      arguments->user= arg;
      break;
    case 'p':
      arguments->pass= arg;
      break;
    case 'd':
      arguments->db= arg;
      break;
    case 'q':
      arguments->max_queries= strtoull(arg, NULL, 10);
      break;
    default:
      return ARGP_ERR_UNKNOWN;
      break;
  }
  return 0;
}

static char doc[] = "AttachBench, a benchmark for MySQL servers";
static char args_doc[] = "";

static struct argp argp = { options, parse_opt, args_doc, doc, NULL, NULL, NULL };

void *thread_start(void *arg)
{
  struct connect_thread *thread_data= (struct connect_thread*)arg;
  attachsql_group_t *group;
  attachsql_error_t *error= NULL;
  struct connect_data *con_data;
  struct arguments *arguments= thread_data->arguments;
  uint16_t con_counter;
  uint64_t query_counter= 0;
  uint64_t query_done_counter= 0;

  printf("Starting thread %d\n", thread_data->thread_num);

  group= attachsql_group_create(NULL);

  con_data= malloc(sizeof(struct connect_data) * arguments->connections);
  if (!con_data)
  {
    printf("Error allocating connection data");
    exit(-1);
  }
  for (con_counter= 0; con_counter < arguments->connections; con_counter++)
  {
    /* Create connections and add them to the group */
    con_data[con_counter].con= attachsql_connect_create(arguments->host, arguments->port, arguments->user, arguments->pass, arguments->db, NULL);
    attachsql_connect_set_callback(con_data[con_counter].con, callbk, &con_data[con_counter]);
    attachsql_group_add_connection(group, con_data[con_counter].con, &error);
    con_data[con_counter].con_no= (thread_data->thread_num * arguments->connections) + con_counter;
    con_data[con_counter].arguments= arguments;
    con_data[con_counter].query_counter= &query_counter;
    con_data[con_counter].query_done_counter= &query_done_counter;
    con_data[con_counter].max_queries= &thread_data->max_queries;
    attachsql_connect(con_data[con_counter].con, &error);
    if (error)
    {
      printf("Setup error: %s", attachsql_error_message(error));
      exit(-1);
    }
  }

  while (query_done_counter < (thread_data->max_queries + arguments->connections))
  {
    attachsql_group_run(group);
  }
  attachsql_group_destroy(group);
  return NULL;
}

int main(int argc, char *argv[])
{
  struct arguments arguments;
  const char *default_host= "localhost";
  const char *default_user= "test";
  const char *default_pass= "";

  uint16_t thread_counter;
  struct connect_thread *threads;

  struct timeval start, stop;
  double start_seconds, stop_seconds, seconds;

  arguments.verbose= false;
  arguments.connections= 4;
  arguments.threads= 4;
  arguments.host= (char*)default_host;
  arguments.port= 3306;
  arguments.user= (char*)default_user;
  arguments.pass= (char*)default_pass;
  arguments.max_queries= 10000;

  argp_parse (&argp, argc, argv, 0, 0, &arguments);

  srand(time(NULL));

  threads= malloc(sizeof(struct connect_thread) * arguments.threads);
  printf("Running %ld queries on %d threads with %d connections per thread\n", arguments.max_queries, arguments.threads, arguments.connections);

  gettimeofday(&start, NULL);

  for (thread_counter= 0; thread_counter < arguments.threads; thread_counter++)
  {
    threads[thread_counter].max_queries= arguments.max_queries / arguments.threads;
    threads[thread_counter].thread_num= thread_counter;
    threads[thread_counter].arguments= &arguments;
    pthread_create(&threads[thread_counter].thread_id, NULL, &thread_start, &threads[thread_counter]);
  }

  for (thread_counter= 0; thread_counter < arguments.threads; thread_counter++)
  {
    pthread_join(threads[thread_counter].thread_id, NULL);
  }

  gettimeofday(&stop, NULL);
  start_seconds= (double)start.tv_sec + ((double)start.tv_usec / 1000000);
  stop_seconds= (double)stop.tv_sec + ((double)stop.tv_usec / 1000000);
  seconds= stop_seconds - start_seconds;

  printf("%ld queries in %.3f seconds, %.3f queries per second\n", arguments.max_queries, seconds, ((float)arguments.max_queries)/seconds);
  return 0;
}

void callbk(attachsql_connect_t *con, attachsql_events_t events, void *context, attachsql_error_t *error)
{
  struct connect_data *con_data= (struct connect_data*)context;
  uint16_t rand_id;

  switch(events)
  {
    case ATTACHSQL_EVENT_CONNECTED:
      if (con_data->arguments->verbose)
      {
        printf("Connection %d connected\n", con_data->con_no);
      }
    case ATTACHSQL_EVENT_EOF:
      attachsql_query_close(con);
      *con_data->query_done_counter= *con_data->query_done_counter + 1;
      if (*con_data->query_counter < *con_data->max_queries)
      {
        rand_id= rand() % 10000;
        snprintf(con_data->query, 40, "SELECT * FROM sbtest WHERE id=%d\n", rand_id);
        if (con_data->arguments->verbose)
        {
          //printf("Sending query from %d: %s\n", con_data->con_no, con_data->query);
        }
        attachsql_query(con, strlen(con_data->query), con_data->query, 0, NULL, &error);
        if (error)
        {
          printf("Error on con %d: %s\n", con_data->con_no, attachsql_error_message(error));
          exit(-1);
        }
        *con_data->query_counter= *con_data->query_counter + 1;
      }
      else
      {
        if (con_data->arguments->verbose)
        {
          printf("Connection %d finished\n", con_data->con_no);
        }
      }

      break;
    case ATTACHSQL_EVENT_ERROR:
      printf("Error on con %d: %s\n", con_data->con_no, attachsql_error_message(error));
      exit(-1);
      break;
    case ATTACHSQL_EVENT_ROW_READY:
      attachsql_query_row_get(con, &error);
      if (error)
      {
        printf("Error on con %d: %s\n", con_data->con_no, attachsql_error_message(error));
        exit(-1);
      }
      if (con_data->arguments->verbose)
      {
        //printf("Con: %d, got row\n", con_data->con_no);
      }
      attachsql_query_row_next(con);
      break;
    case ATTACHSQL_EVENT_NONE:
    default:
      /* should never happen */
      abort();
      break;
  }
}
