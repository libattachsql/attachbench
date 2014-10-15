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

const char *argp_program_version= "attachsql 0.1.0";
const char *argp_bug_address= "<andrew@linuxjedi.co.uk>";

static struct argp_option options[]=
{
  {"verbose", 'v', 0, 0, "Verbose output", 0},
  {"connection", 'c', "CONNECTIONS", 0, "Connections per thread", 0},
  {"host", 'h', "HOST", 0, "Host name or socket path", 0},
  {"port", 'o', "PORT", 0, "Port number (0 for socket)", 0},
  {"user", 'u', "USER", 0, "Username", 0},
  {"pass", 'p', "PASS", 0, "Password", 0},
  {0}
};

struct arguments
{
  bool verbose;
  uint16_t connections;
  char* host;
  uint16_t port;
  char* user;
  char* pass;
};

static error_t parse_opt(int key, char *arg, struct argp_state *state);

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
      arguments->connections= strtoul(arg, NULL, 10);
      break;
    case 'u':
      arguments->user= arg;
      break;
    case 'p':
      arguments->pass= arg;
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

int main(int argc, char *argv[])
{
  struct arguments arguments;

  arguments.verbose= false;
  arguments.connections= 16;
  arguments.host= NULL;
  arguments.port= 3306;
  arguments.user= NULL;
  arguments.pass= NULL;

  argp_parse (&argp, argc, argv, 0, 0, &arguments);
  return 0;
}
