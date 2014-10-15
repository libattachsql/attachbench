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
#include <string.h>
#include <libattachsql-1.0/attachsql.h>

const char *argp_program_version= "attachsql 0.1.0";
const char *argp_bug_address= "<andrew@linuxjedi.co.uk>";

static struct argp_option options[]=
{
  {"verbose", 'v', 0, 0, "Verbose output", 0},
  {"connections", 'c', "CONNECTIONS", 0, "Connections per thread", 0},
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
};


static error_t parse_opt(int key, char *arg, struct argp_state *state);
void callbk(attachsql_connect_t *con, attachsql_events_t events, void *context, attachsql_error_t *error);

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

uint64_t query_done_counter= 0;
uint64_t query_counter= 0;

static struct argp argp = { options, parse_opt, args_doc, doc, NULL, NULL, NULL };

int main(int argc, char *argv[])
{
  struct arguments arguments;
  const char *default_host= "localhost";
  const char *default_user= "test";
  const char *default_pass= "";

  struct connect_data *con_data;
  attachsql_group_t *group;
  attachsql_error_t *error= NULL;
  uint16_t con_counter;

  clock_t start, diff;
  float sec;

  arguments.verbose= false;
  arguments.connections= 16;
  arguments.host= (char*)default_host;
  arguments.port= 3306;
  arguments.user= (char*)default_user;
  arguments.pass= (char*)default_pass;
  arguments.max_queries= 10000;

  argp_parse (&argp, argc, argv, 0, 0, &arguments);

  con_data= malloc(sizeof(struct connect_data) * arguments.connections);
  if (!con_data)
  {
    printf("Error allocating connection data");
    return -1;
  }

  group= attachsql_group_create(NULL);
  srand(time(NULL));

  printf("Running %ld queries on %d connections\n", arguments.max_queries, arguments.connections);

  for (con_counter= 0; con_counter < arguments.connections; con_counter++)
  {
    /* Create connections and add them to the group */
    con_data[con_counter].con= attachsql_connect_create(arguments.host, arguments.port, arguments.user, arguments.pass, arguments.db, NULL);
    attachsql_connect_set_callback(con_data[con_counter].con, callbk, &con_data[con_counter]);
    attachsql_group_add_connection(group, con_data[con_counter].con, &error);
    con_data[con_counter].con_no= con_counter;
    con_data[con_counter].arguments= &arguments;
    attachsql_connect(con_data[con_counter].con, &error);
    if (error)
    {
      printf("Setup error: %s", attachsql_error_message(error));
      return -1;
    }
  }
  start= clock();
  while (query_done_counter < (arguments.max_queries + arguments.connections))
  {
    attachsql_group_run(group);
  }
  diff= clock() - start;
  attachsql_group_destroy(group);
  sec= (float)diff / (float)CLOCKS_PER_SEC;
  printf("%ld queries in %.3f seconds, %.3f queries per second", arguments.max_queries, sec, ((float)arguments.max_queries)/sec);
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
      query_done_counter++;
      if (query_counter < con_data->arguments->max_queries)
      {
        rand_id= rand() % 10000;
        snprintf(con_data->query, 40, "SELECT * FROM sbtest WHERE id=%d\n", rand_id);
        if (con_data->arguments->verbose)
        {
          printf("Sending query from %d: %s", con_data->con_no, con_data->query);
        }
        attachsql_query(con, strlen(con_data->query), con_data->query, 0, NULL, &error);
        if (error)
        {
          printf("Error on con %d: %s", con_data->con_no, attachsql_error_message(error));
          exit(-1);
        }
        query_counter++;
      }
      else
      {
        if (con_data->arguments->verbose)
        {
          printf("Connection %d finished", con_data->con_no);
        }
      }

      break;
    case ATTACHSQL_EVENT_ERROR:
      printf("Error on con %d: %s", con_data->con_no, attachsql_error_message(error));
      exit(-1);
      break;
    case ATTACHSQL_EVENT_ROW_READY:
      attachsql_query_row_get(con, &error);
      if (error)
      {
        printf("Error on con %d: %s", con_data->con_no, attachsql_error_message(error));
        exit(-1);
      }
      if (con_data->arguments->verbose)
      {
        printf("Con: %d, got row\n", con_data->con_no);
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
