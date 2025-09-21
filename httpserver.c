//6.0
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <regex.h>
#include <assert.h>
#include "listener_socket.h"
#include "protocol.h"
#include "queue.h"
#include "rwlock.h"
#include <sys/stat.h>

#define MAX_THREADS       4
#define MAX_QUEUE         1024
#define MAX_BUFFER_SIZE   (2 * 1024 * 1024)
#define DEBUG_ON          1
#define DEBUG_BODY_ON     0
#define DEBUG_PUT_BODY_ON 0

typedef struct request {
    int client_socket;
    int request_id; // unique internal id (from client event)
    long sequence_id;
    char uri[256];
    char method[16];
    char version[16];
    bool has_id;
    int id_header_value; // client-provided Request-Id header value, if any
} request_t;

queue_t *request_queue = NULL;
int thread_count = MAX_THREADS;
pthread_mutex_t global_seq_lock = PTHREAD_MUTEX_INITIALIZER;
long global_sequence = 0;
pthread_mutex_t response_log_mutex = PTHREAD_MUTEX_INITIALIZER;

#define MAX_FILELOCKS 1024
typedef struct {
    char filename[500];
    rwlock_t *lock;
} filelock_entry_t;
filelock_entry_t g_filelocks[MAX_FILELOCKS];
int g_filelocks_count = 0;
pthread_mutex_t g_filelocks_mutex = PTHREAD_MUTEX_INITIALIZER;
rwlock_t *get_lock_for_uri(const char *uri);
void *worker_thread(void *arg);
rwlock_t *log_lock = NULL;

/* void debug_log(const char *format, ...) {
    if(DEBUG_ON){
        va_list args;
        va_start(args, format);
        vprintf(format, args);
        va_end(args);
        fflush(stdout);
    } else return;
} */

void debug_log(const char *format, ...) {
    static FILE *log_file = NULL;

    if (log_file == NULL) {
        log_file = fopen("debug.log", "w"); // "w" mode overwrites the file on each program start
        if (!log_file) {
            perror("Failed to open debug.log");
            return;
        }
    }

    va_list args;
    va_start(args, format);
    vfprintf(log_file, format, args);
    va_end(args);

    fflush(log_file); // Ensure logs are immediately written to the file
}

void send_response_and_log_general(request_t *req, int status_code, const char *body) {
    int client_socket = req->client_socket;
    const char *method = req->method;
    const char *uri = req->uri;
    int audit_id = req->has_id ? req->id_header_value : req->sequence_id;

    // Determine a default message based on the status code if no body is provided.
    const char *default_message;
    switch (status_code) {
    case 200: default_message = "OK"; break;
    case 201: default_message = "Created"; break;
    case 400: default_message = "Bad Request"; break;
    case 404: default_message = "Not Found"; break;
    case 500: default_message = "Internal Server Error"; break;
    default: default_message = "Unknown"; break;
    }
    char *alloc_body = NULL; // Used if we need to allocate memory

    if (body == NULL) {
        // Allocate memory for default message + newline + null terminator.
        size_t len = strlen(default_message) + 2;
        alloc_body = malloc(len);
        if (alloc_body) {
            strcpy(alloc_body, default_message);
            alloc_body[strlen(default_message)] = '\n'; // Append newline.
            alloc_body[strlen(default_message) + 1] = '\0'; // Null-terminate.
            body = alloc_body;
        } else {
            debug_log("[SEND] Memory allocation failed, using default message\n");
            body = "Internal Server Error\n";
            status_code = 500;
        }
    }

    size_t length = strlen(body); // `body` now points to correct response

    const char *status_text = default_message;
    pthread_mutex_lock(&response_log_mutex);
    writer_lock(log_lock);
    fprintf(stderr, "%s,%s,%d,%d\n", method, uri, status_code, audit_id);
    fflush(stderr);
    debug_log("[DEBUG] Audit_log: %s,%s,%d,%d\n\n", method, uri, status_code, audit_id);
    writer_unlock(log_lock);
    pthread_mutex_unlock(&response_log_mutex);

    dprintf(client_socket,
        "HTTP/1.1 %d %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n"
        "%s",
        status_code, status_text, length, body);
    if (DEBUG_BODY_ON) {
        debug_log("[DEBUG] Response: HTTP/1.1 %d %s\r\n"
                  "\tContent-Length: %zu\r\n"
                  "\tConnection: close\r\n"
                  "\t\r\n"
                  "\t%s\n",
            status_code, status_text, length, body);

    } else {
        debug_log("[DEBUG] Response: HTTP/1.1 %d %s\r\n"
                  "\tContent-Length: %zu\r\n"
                  "\tConnection: close\r\n"
                  "\t\r\n"
                  "\t<body>\n",
            status_code, status_text, length);
    }
}

void enqueue_request(int client_socket) {
    request_t *req = calloc(1, sizeof(request_t));
    req->client_socket = client_socket;
    /* Generate a unique internal request id from the client event */
    req->request_id = rand(); // or you could use a more deterministic generator if needed
    pthread_mutex_lock(&global_seq_lock);
    req->sequence_id = global_sequence;
    global_sequence++;
    pthread_mutex_unlock(&global_seq_lock);
    bool pushed = queue_push(request_queue, req);
    if (!pushed) {
        close(client_socket);
        free(req);
        debug_log("[ERROR] Request queue full, dropping connection\n");
    }
}

request_t *dequeue_request(void) {
    request_t *req = NULL;
    bool popped = queue_pop(request_queue, (void **) &req);
    if (!popped) {
        return NULL;
    }
    return req;
}

int parse_request_line(const char *line, char *method, char *uri, char *version) {
    regex_t regex;
    char pattern[] = "^(" TYPE_REGEX ") (" URI_REGEX ") (" HTTP_REGEX ")\r?\n";
    if (regcomp(&regex, pattern, REG_EXTENDED)) {
        return 500;
    }
    regmatch_t matches[4];
    int rc = regexec(&regex, line, 4, matches, 0);
    regfree(&regex);
    if (rc == 0) {
        int len1 = matches[1].rm_eo - matches[1].rm_so;
        strncpy(method, line + matches[1].rm_so, len1);
        method[len1] = '\0';
        int len2 = matches[2].rm_eo - matches[2].rm_so;
        strncpy(uri, line + matches[2].rm_so, len2);
        uri[len2] = '\0';
        int len3 = matches[3].rm_eo - matches[3].rm_so;
        strncpy(version, line + matches[3].rm_so, len3);
        version[len3] = '\0';

        return 0;
    } else {
        return 400;
    }
}

int read_request_headers(const char *buf, int *reqid_val, bool *has_reqid) {
    *has_reqid = 0;
    *reqid_val = 0;
    const char *p = strstr(buf, "\n");
    if (!p)
        return 0;
    p++; // move past the request line
    while (*p) {
        if (!strncmp(p, "\r\n", 2) || !strncmp(p, "\n", 1)) {
            break; // reached end of headers
        }
        if (!strncasecmp(p, "Request-Id:", 11)) {
            p += 11;
            while (*p == ' ' || *p == '\t')
                p++;
            if (*p != '\r' && *p != '\n' && *p != '\0') {
                int val = atoi(p);
                *reqid_val = val;
                *has_reqid = 1;
            }
        }
        const char *nl = strstr(p, "\n");
        if (!nl)
            break;
        p = nl + 1;
    }
    debug_log("[DEBUG] Request-Id: reqid_val = %d, has_reqid = %d\n", *reqid_val, *has_reqid);
    return 0;
}

/* int execute_get(const char *location, char **body) {//get1.0
    int fd = open(location + 1, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return 404;
        } else {
            return 500;
        }
    }
    char *buffer = malloc(MAX_BUFFER_SIZE);
    if (!buffer) {
        close(fd);
        return 500;
    }
    ssize_t total = 0, bytes_read;
    while ((bytes_read = read(fd, buffer, MAX_BUFFER_SIZE)) > 0) {
        total += bytes_read;
    }
    if (bytes_read < 0) {
        free(buffer);
        close(fd);
        return 500;
    }
    lseek(fd, 0, SEEK_SET);
    while ((bytes_read = read(fd, buffer, MAX_BUFFER_SIZE)) > 0) {
        ssize_t off = 0;
        while (off < bytes_read) {
            ssize_t wr = write(client_socket, buffer + off, bytes_read - off);
            if (wr < 0) {
                break;
            }
            off += wr;
        }
    }
    free(buffer);
    close(fd);
    return 200;
} */

/* int execute_get(const char *location, char **body, size_t *body_length) {
    // Open the file; skip the leading '/'.
    int fd = open(location + 1, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return 404;
        } else {
            return 500;
        }
    }

    // Use fstat() to determine the file size.
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return 500;
    }
    *body_length = st.st_size;
    debug_log("[DEBUG] [GET] body length = %d\n", *body_length);
    // Allocate a buffer to hold the entire file.
    *body = malloc(*body_length);
    if (!*body) {
        close(fd);
        return 500;
    }

    // Read the file content into the allocated buffer.
    ssize_t total_read = 0;
    while (total_read < (ssize_t)*body_length) {
        ssize_t bytes_read = read(fd, *body + total_read, *body_length - total_read);
        if (bytes_read < 0) {
            free(*body);
            close(fd);
            return 500;
        }
        total_read += bytes_read;
    }

    close(fd);
    return 200;
} */

// New helper function to read the HTTP header byte-by-byte.
int read_http_header(int client_socket, char *header, size_t max_size) {
    size_t index = 0;
    char c;
    int n;
    while (index < max_size - 1) {
        n = read(client_socket, &c, 1);
        if (n <= 0) {
            // Error or connection closed.
            break;
        }
        header[index++] = c;
        // Check if the last four characters form "\r\n\r\n"
        if (index >= 4 && header[index - 4] == '\r' && header[index - 3] == '\n'
            && header[index - 2] == '\r' && header[index - 1] == '\n') {
            break;
        }
    }
    header[index] = '\0';
    return index;
}

int execute_get(const char *location, char **body) {
    int fd = open(location + 1, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return 404;
        } else {
            return 500;
        }
    }

    size_t capacity = MAX_BUFFER_SIZE;
    size_t total = 0;
    char *buffer = malloc(capacity);
    if (!buffer) {
        close(fd);
        return 500;
    }

    ssize_t bytes_read;
    while ((bytes_read = read(fd, buffer + total, capacity - total)) > 0) {
        total += bytes_read;
        // If the buffer is full, double its capacity.
        if (total == capacity) {
            capacity *= 2;
            char *new_buffer = realloc(buffer, capacity);
            if (!new_buffer) {
                free(buffer);
                close(fd);
                return 500;
            }
            buffer = new_buffer;
        }
    }
    if (bytes_read < 0) {
        free(buffer);
        close(fd);
        return 500;
    }

    // Optionally, shrink the buffer to the exact size needed plus one byte for null termination.
    char *final_buffer = realloc(buffer, total + 1);
    if (final_buffer != NULL) {
        buffer = final_buffer;
    }
    buffer[total] = '\0'; // Null-terminate the string.
    *body = buffer;

    close(fd);
    return 200;
}

//put 1.0
/* int execute_put(const char *uri, const char *request_buf, int client_socket) {
    debug_log("[PUT] Entered execute_put\n");
    const char *cl = strcasestr(request_buf, "Content-Length:");
    if (!cl) {
        return 400;
    }
    cl += strlen("Content-Length:");
    while (*cl == ' ' || *cl == '\t')
        cl++;
    int length = atoi(cl);
    if (length < 0 || length > MAX_BUFFER_SIZE) {
        return 400;
    }
    debug_log("[PUT] Length to put = %d\n", length);
    char *body = malloc(length);
    if (!body){
        debug_log("[PUT] char *body allocation failed\n");
        return 500;
    }
    debug_log("[PUT] *body allocate successed\n");
    int total_read = 0;
    while (total_read < length) {
        int r = read(client_socket, body + total_read, length - total_read);
        debug_log("[DEBUG] Read %d bytes (expected %d bytes total, currently at %d)\n", r, length, total_read + r);
        
        if (r <= 0) {
            debug_log("[DEBUG] Read failed or client closed connection. Returning 400\n");
            free(body);
            return 400;
        }
        total_read += r;
    }

    int fd = open(uri + 1, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        free(body);
        debug_log("[PUT] File open failed failed\n");
        return 500;
    }
    int w = write(fd, body, length);
    debug_log("[PUT] Put length = %d\n", length);
    if(DEBUG_PUT_BODY_ON){
        
        debug_log("[PUT] Content:%s\n", body);
    }
    close(fd);
    free(body);
    if (w < 0) {
        debug_log("[PUT] File write failed failed\n");
        return 500;
    }
    return 201;
}
 */

//put 2.0
int execute_put(request_t *req, const char *header_buf, int client_socket, rwlock_t *rw) {
    debug_log("[PUT] Entered execute_put\n");
    const char *uri = req->uri;
    // Parse Content-Length from the header.
    const char *cl = strstr(header_buf, "Content-Length:");
    if (!cl) {
        debug_log("[PUT] Missing Content-Length header\n");
        return 400;
    }
    cl += strlen("Content-Length:");
    while (*cl == ' ' || *cl == '\t')
        cl++;
    int content_length = atoi(cl);
    if (content_length <= 0 || content_length > MAX_BUFFER_SIZE) {
        debug_log("[PUT] Invalid Content-Length: %d\n", content_length);
        return 400;
    }
    debug_log("[PUT] Parsed Content-Length = %d\n", content_length);

    // Determine if the file exists.

    // Allocate a buffer for the body.
    char *body = malloc(content_length + 1);
    if (!body) {
        debug_log("[PUT] Memory allocation failed\n");
        return 500;
    }
    int total_read = 0;
    // Read the body from the socket.
    while (total_read < content_length) {
        int r = read(client_socket, body + total_read, content_length - total_read);
        if (r <= 0) {
            debug_log(
                "[PUT] Read error or client closed connection before full body was received\n");
            free(body);
            return 500;
        }
        total_read += r;
        debug_log("[PUT] Read %d/%d bytes\n", total_read, content_length);
    }
    body[content_length] = '\0'; // Null-terminate for safety.

    writer_lock(rw);
    int status_code;
    struct stat st;
    if (stat(uri + 1, &st) == 0) {
        // File exists; subsequent PUTs should return 200.
        status_code = 200;
    } else {
        // File doesn't exist; this PUT creates it.
        status_code = 201;
    }
    // Open the file (using O_TRUNC to replace its contents).
    int fd = open(uri + 1, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        debug_log("[PUT] Failed to open file: %s\n", uri);
        free(body);
        return 500;
    }
    int written = write(fd, body, content_length);
    writer_unlock(rw);

    if (written < 0) {
        debug_log("[PUT] Failed to write to file: %s\n", uri);
        close(fd);
        free(body);
        return 500;
    }

    debug_log("[PUT] Successfully wrote %d bytes to file: %s\n", written, uri);
    close(fd);
    free(body);
    send_response_and_log_general(req, status_code, NULL);

    return status_code;
}

void handle_request(request_t *req) {
    char header_buffer[MAX_BUFFER_SIZE];
    // Read only the header (up to "\r\n\r\n") from the socket.
    int header_length = read_http_header(req->client_socket, header_buffer, sizeof(header_buffer));
    if (header_length <= 0) {
        send_response_and_log_general(req, 500, NULL);
        return;
    }

    // Parse the request line from the header.
    int pr = parse_request_line(header_buffer, req->method, req->uri, req->version);
    if (pr != 0) {
        send_response_and_log_general(req, 400, NULL);
        return;
    }

    // Validate HTTP version.
    if (strcmp(req->version, HTTP_VERSION) != 0) {
        send_response_and_log_general(req, 400, NULL);
        return;
    }

    // Parse additional headers (like Request-Id).
    read_request_headers(header_buffer, &req->id_header_value, &req->has_id);
    debug_log("[DEBUG] Parsing done:\n");
    debug_log("\t- Method: %s\n", req->method);
    debug_log("\t- uri: %s\n", req->uri);
    debug_log("\t- Version: %s\n", req->version);
    debug_log("\t- id_header_value: %d\n", req->id_header_value);
    debug_log("\t- has_id: %d\n", req->has_id);

    // Get the file lock based on URI.
    rwlock_t *rw = get_lock_for_uri(req->uri);
    if (!rw) {
        send_response_and_log_general(req, 500, NULL);
        return;
    }

    int status_code = 0;
    if (strcasecmp(req->method, "GET") == 0) {
        char *body = NULL;
        reader_lock(rw);
        status_code = execute_get(req->uri, &body);
        reader_unlock(rw);
        if (status_code != 200) {
            send_response_and_log_general(req, status_code, NULL);
        } else {
            send_response_and_log_general(req, 200, body);
        }

        free(body);
    } else if (strcasecmp(req->method, "PUT") == 0) {

        // For PUT, the header_buffer now contains ONLY the header.
        status_code = execute_put(req, header_buffer, req->client_socket, rw);

        // send_response_and_log_general(req, status_code, NULL);
        // writer_unlock(rw);
    } else {
        send_response_and_log_general(req, 400, NULL);
    }
}

void *worker_thread(void *arg) {
    (void) arg;
    while (1) {
        request_t *req = dequeue_request();
        if (!req) {
            continue;
        }
        handle_request(req);
        close(req->client_socket);
        free(req);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    int port, opt;
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        if (opt == 't') {
            thread_count = atoi(optarg);
        }
    }
    if (optind >= argc) {
        fprintf(stderr, "Usage: %s [-t threads] <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    port = atoi(argv[optind]);
    request_queue = queue_new(MAX_QUEUE);
    assert(request_queue);
    pthread_t workers[thread_count];
    for (int i = 0; i < thread_count; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }
    log_lock = rwlock_new(WRITERS, 0);
    Listener_Socket_t *listener = ls_new(port);
    if (!listener) {
        perror("Failed to create listener socket");
        debug_log("[DEBUG] Failed to create listener socket\n");
        exit(EXIT_FAILURE);
    }
    while (1) {
        int client_socket = ls_accept(listener);
        if (client_socket < 0) {
            perror("Failed to accept connection");
            debug_log("[DEBUG] Failed to accept connection, socket=%d\n", client_socket);
            continue;
        }
        enqueue_request(client_socket);
    }
    ls_delete(&listener);
    return 0;
}

rwlock_t *get_lock_for_uri(const char *uri) {
    pthread_mutex_lock(&g_filelocks_mutex);
    for (int i = 0; i < g_filelocks_count; i++) {
        if (strcmp(g_filelocks[i].filename, uri) == 0) {
            rwlock_t *res = g_filelocks[i].lock;
            pthread_mutex_unlock(&g_filelocks_mutex);
            return res;
        }
    }
    if (g_filelocks_count >= MAX_FILELOCKS) {
        pthread_mutex_unlock(&g_filelocks_mutex);
        return NULL;
    }
    strncpy(g_filelocks[g_filelocks_count].filename, uri, sizeof(g_filelocks[0].filename) - 1);
    g_filelocks[g_filelocks_count].filename[sizeof(g_filelocks[0].filename) - 1] = '\0';
    // Use N_WAY priority with n=2.
    g_filelocks[g_filelocks_count].lock = rwlock_new(N_WAY, 2);
    rwlock_t *out = g_filelocks[g_filelocks_count].lock;
    g_filelocks_count++;
    pthread_mutex_unlock(&g_filelocks_mutex);
    return out;
}
