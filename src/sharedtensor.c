// build@ gcc -shared -I/home/sdonovan/lua/include -o mylib.so mylib.c
// includes for your code
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <stdlib.h>

// includes for Lua
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

// network stuff
#include <netdb.h>
#include <netinet/in.h>
#include <errno.h>

// threading
#include <pthread.h>

#include <TH/THTensor.h>
#include <luaT.h>

typedef struct SharedTensor;

typedef struct {
  SharedTensor* s;  // backlink to enclosing structure
  int fd;
  int closing;    // set to 1 during shutdown.
  int in_count;   // how many complete inward cycles
  int out_count;  // how many complete outward cycles.
  pthread_t in_thread;
  pthread_t out_thread;
  pthread_cond_t state_change;  // fired for all state changes
  pthread_mutex_t mutex;    // protects all fields
  
  float* delta;   // remaining delta to be sent down this connection
} Connection;

typedef struct {
  Connection c[4];
  int valueslen;
  int listen_fd;
  pthread_t listen_thread;
} SharedTensor;

typedef struct {  // Used to pack params for thread creation.
  SharedTensor* s;
  Connection* c;
} SyncParams;

void read_or_close(Connection* c, char *buf, size_t count) {
  int res = 0;
  while(count && !c->closing) {
    res = read(c->`fd, (char*)buf, count);
    if (res == -1)
      if (errno == EINTR)
        continue;
      else {
        c->closing = 1;
        perror("read from socket");
      }
        
    count -= res;
    buf += res;
  }
}

void write_or_close(Connection* c, char *buf, size_t count) {
  int res = 0;
  while(count && !c->closing) {
    res = write(c->fd, (char*)buf, count);
    if (res == -1)
      if (errno == EINTR)
        continue;
      else {
        perror("write to socket");
        c->closing = 1;
      }
        
    count -= res;
    buf += res;
  }
}


void save_deltas(unsigned char* data, float* values, int valueslen, float scale) {
  int i;
  for (i=0; i<valueslen; i++) {
    values[i] += scale - ((data[i/8]>>(i%8))&1)*scale*2;
  }
}

void * sync_in(void* void_sp) {
  Connection* c = (Connection*)void_c;
  
  int valueslen = c->s->valueslen;
  int buflen = (valueslen+7)/8;
  char buf[buflen];
  while (!c->closing) {
    float scale;
    read_or_close(c, (char*)&scale, sizeof(float));
    read_or_close(c, buf, buflen);
    if (c->closing) break;
        
    if (&c->s->left != c) save_deltas(buf, c->s->left.delta, valueslen, scale); 
    if (&c->s->right != c) save_deltas(buf, c->s->right.delta, valueslen, scale);
    if (&c->s->up != c) save_deltas(buf, c->s->up.delta, valueslen, scale);
    save_deltas(buf, c->s->values, valueslen, scale);
    
  }

}

void * sync_out(void* void_c) {
  Connection* c = (Connection*)void_c;
  
  int valueslen = c->s->valueslen;
  float* values = c->delta;
  int buflen = (valueslen+7)/8;
  char buf[buflen];
  while (!c->closing) {
    // figure out RMS magnitude
    // It's unclear the perfect scale number here
    // A smaller number would be good for getting
    // most of the values very accurate quickly.
    // A larger number would be best for getting all
    // the numbers closeish quickly.
        
    int i;
    float scale = 0;

    for (i=0; i<valueslen; i++)
      scale += c->delta[i] * c->delta[i];
    scale = sqrt(scale / valueslen);
    scale = pow(2,floor(log2(scale)));
    
    // TODO:  halt and reawaken later if scale == 0
    if (scale==0) {
      sleep(1);
    }
    
    bzero(buf, buflen);
    for (i=0; i<valueslen; i++) {
      if (values[i] > 0) {
        values[i] -= scale;
      } else {
        buf[i/8] |= 1<<(i%8);
        values[i] += scale;
      }
    }
    
    write_or_die(c->fd, (char*)&scale, sizeof(float));
    write_or_die(c->fd, buf, buflen);
    
  }
  
  if(pthread_join(in_thread, NULL)) {
    fprintf(stderr, "Error joining thread\n");
  }
  
  close(c->fd);
  free((void*)sp);
}


void set_up_connection(Connection* c, SharedTensor* s, int fd) {
  pthread_cond_init(&c->state_change, NULL);
  pthread_mutex_init(&c->mutex, NULL);
  pthread_mutex_lock(&c->mutex);
  c->s = s;
  c->fd = fd;
  c->closing = 0;
  c->in_count = 0;
  c->out_count = 0;
  c->in_thread = NULL;
  c->out_thread = NULL;
  c->delta = (float*)malloc(c->s->valueslen * sizeof(float));
  pthread_create(&c->in_thread, NULL, sync_in, c)) {
  pthread_create(&c->out_thread, NULL, sync_out, c)) {
  pthread_mutex_unlock(&c->mutex);
}

void tear_down_connection(Connection* c) {
  pthread_mutex_lock(&c->mutex);
  c->closing = 1;  
  pthread_mutex_unlock(&c->mutex);
  
  pthread_join(c->in_thread, NULL);
  pthread_join(c->out_thread, NULL);
  
  free(c->delta);
  close(c->fd);
  pthread_mutex_destroy(&c->mutex);
}



void * do_listening(void* void_s) {
   SharedTensor* s = (SharedTensor*)void_s;
   SyncParams* p_sp;
   
   struct sockaddr_in left_addr, right_addr, unused_addr;
   socklen_t unused_addrlen = sizeof(left_addr);
   
   
   // connect this guy to left side
   s->left.fd = accept(s->listen_fd, (struct sockaddr*)&left_addr, &unused_addrlen);
   write_or_die(s->left.fd,"Y",1);
   pthread_t left_thread=0;
   p_sp = (SyncParams*)malloc(sizeof(SyncParams));
   p_c->s = s;
   p_c = &s->left;
   if(pthread_create(&left_thread, NULL, synca, p_sp)) {
     fprintf(stderr, "Error creating thread\n");
   }
   
   // connect this guy to right side
   s->right.fd = accept(s->listen_fd, (struct sockaddr*)&right_addr, &unused_addrlen);
   write_or_die(s->right.fd,"Y",1);
   pthread_t right_thread=0;
   p_sp = (SyncParams*)malloc(sizeof(SyncParams));
   p_c->s = s;
   p_c = &s->right;
   if(pthread_create(&right_thread, NULL, synca, p_sp)) {
     fprintf(stderr, "Error creating thread\n");
   }
   
   // reject all future connections with redirects to children.
   int lrcounter=0;
   while(1) {
     int newfd = accept(s->listen_fd, (struct sockaddr*)&unused_addr, &unused_addrlen);
     if (newfd<0) break;
     write_or_die(newfd,"N",1);
     struct sockaddr_in* addr_to_give = lrcounter++&1?&left_addr:&right_addr;
     write_or_die(newfd,(char*)addr_to_give,sizeof(left_addr));
     
     close(newfd);
   }
   if(pthread_join(left_thread, NULL)) {
     fprintf(stderr, "Error joining thread\n");
   }
   if(pthread_join(right_thread, NULL)) {
     fprintf(stderr, "Error joining thread\n");
   }
}

int connect_to(SharedTensor* s, const char* host, const int port) {
   struct sockaddr_in serv_addr;
   socklen_t serv_addrlen = sizeof(serv_addr);
   struct hostent *server;
   
   int yes = 1;
   
   server = gethostbyname(host);
   if (server == NULL) { fprintf(stderr,"ERROR, no such host\n"); return -1; }
   
   bzero((char *) &serv_addr, sizeof(serv_addr));
   serv_addr.sin_family = AF_INET;
   bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
   serv_addr.sin_port = htons(port);
   
   do {
     /* Create a socket point */
     s->up.fd = socket(AF_INET, SOCK_STREAM, 0);
     if (s->up.fd < 0) { fprintf(stderr, "ERROR opening socket\n"); return -1; }
     
     if (setsockopt(s->up.fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
       perror("setsockopt");
       return -1;
     }
     
     
     /* Now connect to the server */
     if (connect(s->up.fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
       // this case indicates we failed to connect.
       // We don't report the error, because we might be master.
       close(s->up.fd);
       s->up.fd = -1;
       break;
     }

     // successfully connected, but do we need to move?
     char buf;
     read_or_die(s->up.fd,&buf,1);
     
     if (buf=='Y') {
       pthread_t up_thread;
       SyncParams* upsp = (SyncParams*)malloc(sizeof(SyncParams));
       upc->s = s;
       upc = &s->up;
       if(pthread_create(&up_thread, NULL, synca, upsp)) {
         fprintf(stderr, "Error creating thread\n");
       }
       
       // 
       if (getsockname(s->up.fd, (struct sockaddr*)&serv_addr, &serv_addrlen)) {
         perror("ERROR finding own socket");
       };
       break;
     }
     
     // Weren't accepted, find who to connect to next lower down the tree
     if (read(s->up.fd, &serv_addr, sizeof(serv_addr)) != sizeof(serv_addr)) fprintf(stderr, "Conn died\n");;
     close(s->up.fd);
   } while (1);
   
   /* start listening at same endpoint */
   s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
   if (s->listen_fd < 0) { perror("socket");}
	 
   if (setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
     perror("setsockopt");
     return -1;
   }
	 if (bind(s->listen_fd, (struct sockaddr*)&serv_addr, serv_addrlen)) {
	   perror("bind");
	   
     fprintf(stderr, "Likley you specified an address to connect to which is neither on this machine nor running a copy of sharedtensor.  Alternatively, there is a network issue with one of the clients already connected and we can't contact them.\n"); 
     return -1;
   }
	 
	 if (s->up.fd == -1) {
	   fprintf(stderr, "Master Tensor.  Connect more machines to %s:%d for faster learning.\n", host, port);
   } else {
	   fprintf(stderr, "Successfully connected to %s:%d (and other clients).\n", host, port);     
   }
   listen(s->listen_fd, 5);
   
   // create new thread to accept new connections
   if(pthread_create(&s->listen_thread, NULL, do_listening, s)) { 
     fprintf(stderr, "Error creating listen thread\n");
     return -1;
   }
  
   return 0;
}

void addFromInternal(SharedTensor* me, THFloatTensor* from) {
  if (me->valueslen != THFloatTensor_nElement(from)) THError("Not the right size!");
  int i=0;
  float* data = THFloatTensor_data(from);
  for (i=0; i<me->valueslen; i++) {
    me->up.delta[i] += data[i];
    me->left.delta[i] += data[i];
    me->right.delta[i] += data[i];
    me->values[i] += data[i];
  }
}

// defining functions callable from Lua
static int l_createOrFetch (lua_State *L) {
  // check params
  const int port = luaL_checkint(L, 2);
  const char* host = luaL_checkstring(L, 1);
  
  THFloatTensor* input = luaT_checkudata(L, 3, "torch.FloatTensor");
  
    
  // allocate our datastructures
  SharedTensor *me = (SharedTensor *)lua_newuserdata(L, sizeof(SharedTensor));
  luaL_getmetatable(L, "sharedtensor.sharedtensor");
  lua_setmetatable(L, -2);
      
  bzero(me, sizeof(SharedTensor));
  
  me->valueslen = THFloatTensor_nElement(input);
  me->up.delta = malloc(me->valueslen*sizeof(float));
  me->left.delta = malloc(me->valueslen*sizeof(float));
  me->right.delta = malloc(me->valueslen*sizeof(float));
  me->values = malloc(me->valueslen*sizeof(float));
  
  // Try to connect to given host/port
  connect_to(me, host, port);
  
  if (me->up.fd<0) {
    // we have no parent, so should add in the provided tensor.
    addFromInternal(me, input);
  }
  
  return 1;
}


// defining functions callable from Lua
static int l_gc (lua_State *L) {
  SharedTensor *me = (SharedTensor *)lua_touserdata(L, 1);
  
  // TODO:  Fix this mess.  Currently, this object is un-gc-able.
  fprintf(stderr, "You tried to destroy a sharedtensor.  Due to my foolish coding, there isn't support for this client to tell all the other clients connected to it that they need to reconnect to other nodes.  If I disconnect now, they'll get all out of sync.  Hence, everyone on this sharedtensor will crash now to avoid inconsistency.  Patches welcome :-)\n");
  exit(-1);
  
  // close all the sockets to cause all threads to stop and die
  // TODO:  This isn't technically valid - someone else could
  // reuse the same fd number before the thread dies.
  close(me->up.fd);
  close(me->left.fd);
  close(me->right.fd);
  close(me->listen_fd);
    
  // rejoin tree of threads
  if(pthread_join(me->listen_thread, NULL)) {
    fprintf(stderr, "Error joining thread\n");
  }
  
  free(me->up.delta);
  free(me->left.delta);
  free(me->right.delta);
  free(me->values);
  
  return 1;
}
static int l_copyToTensor(lua_State *L) {
  SharedTensor *me = (SharedTensor *)lua_touserdata(L, 1);
  THFloatTensor* input = luaT_checkudata(L, 2, "torch.FloatTensor");
  
  if (me->valueslen != THFloatTensor_nElement(input)) THError("Not the right size!");
  int i=0;
  float* data = THFloatTensor_data(input);
  for (i=0; i<me->valueslen; i++) {
    data[i] = me->values[i];
  }
  return 0;
}

static int l_addFromTensor(lua_State *L) {
  SharedTensor *me = (SharedTensor *)lua_touserdata(L, 1);
  THFloatTensor* input = luaT_checkudata(L, 2, "torch.FloatTensor");
  addFromInternal(me, input);
  return 0;
}

static const luaL_reg mylib[] = {
    {"createOrFetch",l_createOrFetch},
    {NULL,NULL}
};

static const luaL_reg arraylib_m[] = {
    {"__gc",l_gc},
    {"copyToTensor",l_copyToTensor},
    {"addFromTensor",l_addFromTensor},
    {NULL,NULL}
};

int luaopen_sharedtensor(lua_State *L)
{
    luaL_newmetatable(L, "sharedtensor.sharedtensor");
    lua_pushvalue(L, -1); // there are two 'copies' of the metatable on the stack
    lua_setfield(L, -2, "__index"); // pop one of those copies and assign it to
                                    // __index field of the 1st metatable
    luaL_register(L, NULL, arraylib_m);
    luaL_register (L, "sharedtensor", mylib);
    
    return 1;
}
