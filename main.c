#include <rados/librados.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

void print_err(const char *op, int err_code) {
  fprintf(stderr, "%s failed: %d\n", op, err_code);
}

void validate_not_empty(const char *name, const char *val) {
  if (!val || strlen(val) == 0) {
    fprintf(stderr, "%s may not be empty\n", name);
    exit(1);
  }
}

typedef enum rt_op { RT_OP_ADD, RT_OP_REM } rt_op_t;

rt_op_t validate_and_parse_op(const char *op_str) {
  if (strcmp(op_str, "add") == 0) {
    return RT_OP_ADD;
  } else if (strcmp(op_str, "rem") == 0) {
    return RT_OP_REM;
  }

  fprintf(stderr,
          "Unknown operation passed in -o %s. Valid operations are 'add' and "
          "'rem'.\n",
          op_str);
  exit(1);
}

char *mkstring(const char *src, int len) {
  char *s = malloc(len + 1);
  memcpy(s, src, len);
  s[len] = '\0';

  return s;
}

char **tokenize(const char *src, int token, int *token_count) {
  char *str = strdup(src);
  char **tokens = NULL;
  int ntokens = 0;

  char *beg = str;
  char *end = strchr(str, token);
  while (end) {
    ntokens++;

    char *part = mkstring(beg, end - beg);
    tokens = realloc(tokens, sizeof(char *) * ntokens);
    tokens[ntokens - 1] = part;

    beg = end + 1;
    end = strchr(beg, token);
  }

  // Last part.

  ntokens++;

  char *part = mkstring(beg, strlen(beg));
  tokens = realloc(tokens, sizeof(char *) * ntokens);
  tokens[ntokens - 1] = part;

  *token_count = ntokens;

  free(str);
  return tokens;
}

void print_usage(const char *progname) {
  printf("rados-reference-tracker is a proof-of-concept implementation of a "
         "reference tracker for ceph-csi plugin.\n\n");

  printf("Usage: %s -i CLIENT ID -p POOL NAME -c CEPH CONFIG FILE "
         "[-r RT NAME] -k REF KEYS -o RT OPERATION [-h]\n",
         progname);

  printf("  -i CLIENT ID\t\tcephx client ID.\n");
  printf("  -p POOL NAME\t\tCeph pool name.\n");
  printf("  -c CEPH CONFIG FILE\tCeph config file.\n");
  printf("  -r RT NAME\t\tName of the RADOS object for this reference tracker. "
         "Defaults to 'hello-reference-tracker' if none provided.\n");
  printf("  -k REF KEYS\t\tComma-separated list of keys to be used in the RT "
         "operation.\n");
  printf("  -o RT OPERATION\tAccepted values are 'add' and 'rem'. Specifies "
         "what to do with provided keys. 'add' adds them to tracked "
         "references, 'rem' removes them.\n");
  printf("  -h\t\t\tThis help message.\n");
}

int main(int argc, const char **argv) {
  int ret = 0;

  const char *client_id = NULL;
  const char *pool_name = NULL;
  const char *config_file = NULL;
  const char *keys_str = NULL;
  const char *op_str = NULL;
  const char *rt_name = NULL;
  rt_op_t op;

  rados_t rados = NULL;

  // Parse reference-tracker command line options.
  {
    int c;
    while ((c = getopt(argc, (char *const *)argv, "i:p:c:k:o:r:h")) != -1) {
      switch (c) {
      case 'i':
        client_id = optarg;
        break;
      case 'p':
        pool_name = optarg;
        break;
      case 'c':
        config_file = optarg;
        break;
      case 'r':
        rt_name = optarg;
        break;
      case 'k':
        keys_str = optarg;
        break;
      case 'o':
        op_str = optarg;
        break;
      case 'h':
        print_usage(argv[0]);
        exit(0);
      }
    }
  }

  validate_not_empty("-i CLIENT ID", client_id);
  validate_not_empty("-c CEPH CONFIG FILE", client_id);
  validate_not_empty("-p POOL NAME", pool_name);
  validate_not_empty("-k COMMA SEPARATED LIST OF KEYS", keys_str);
  validate_not_empty("-o OPERATION", op_str);
  op = validate_and_parse_op(op_str);

  if (!rt_name || strlen(rt_name) == 0) {
    rt_name = "hello-reference-tracker";
  }

  int keys_count;
  char **keys = tokenize(keys_str, ',', &keys_count);

  // Initialize RADOS.
  {
    ret = rados_create(&rados, client_id);
    if (ret < 0) {
      print_err("rados_create()", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
  }

  // Parse Ceph config.
  {
    ret = rados_conf_parse_argv(rados, argc, argv);
    if (ret < 0) {
      print_err("rados_conf_parse_argv()", ret);
      ret = EXIT_FAILURE;
      goto out;
    }

    if (config_file) {
      ret = rados_conf_read_file(rados, config_file);
      if (ret < 0) {
        print_err("rados_conf_read_file()", ret);
        ret = EXIT_FAILURE;
        goto out;
      }
    }
  }

  // Connect to the cluster.
  {
    ret = rados_connect(rados);
    if (ret < 0) {
      print_err("rados_connect()", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
  }

  // Create IO context.
  {
    ret = rados_ioctx_create(rados, pool_name, &ioctx);
    if (ret < 0) {
      print_err("rados_ioctx_create()", ret);
      ret = EXIT_FAILURE;
      goto out;
    }
  }

out:
  if (ioctx) {
    rados_ioctx_destroy(ioctx);
  }

  rados_shutdown(rados);

  for (int i = 0; i < keys_count; i++) {
    free(keys[i]);
  }
  free(keys);

  return ret;
}
