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
#include <signal.h>
#include <TH/THTensor.h>
#include <luaT.h>

typedef struct {
  int fd;
  int closing;   // set to 1 during shutdown.
  float* delta;   // remaining delta to be sent down this connection
} Connection;

typedef struct {
  Connection up;
  Connection left;
  Connection right;
  int valueslen;
  float* values;
  int listen_fd;
  pthread_t listen_thread;
  int closing;
} SharedTensor;

typedef struct {  // Used to pack params for thread creation.
  SharedTensor* s;
  Connection* c;
} SyncParams;

// noop handler to force accept and read() to return;
void sighand(int signo)
{
  return;
}


void read_or_die(int fd, unsigned char *buf, size_t count) {
  int res = 0;
  while(count) {
    res = read(fd, buf, count);
    if (res == -1) {
      if (errno == EINTR)
        continue;
      else {
        perror("read from socket");
        exit(-1);
      }
    }
        
    count -= res;
    buf += res;
  }
}

void write_or_die(int fd, unsigned char *buf, size_t count) {
  int res = 0;
  while(count) {
    res = write(fd, buf, count);
    if (res == -1) {
      if (errno == EINTR)
        continue;
      else {
        perror("write to socket");
        exit(-1);
      }
    }
        
    count -= res;
    buf += res;
  }
}

int accept_or_die(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int* closing) {
  int res=-1;
  while(res<0) {
    res = accept(sockfd, addr, addrlen);
    if (*closing) return -1;
    if (res == -1) {
      if (errno == EINTR) {
        continue; 
      } else {
        perror("accept");
        exit(-1);
      }
    }
  }
  return res;
}

void save_deltas(unsigned char* data, float* values, int valueslen, float scale) {
  int i;
  for (i=0; i<valueslen; i++) {
    values[i] += scale - ((data[i/8]>>(i%8))&1)*scale*2;
  }
}

void * sync_in(void* void_sp) {
  SyncParams* sp = (SyncParams*)void_sp;
  
  int valueslen = sp->s->valueslen;
  int buflen = (valueslen+7)/8;
  unsigned char buf[buflen];
  while (!sp->c->closing) {
    float scale;
    read_or_die(sp->c->fd, (unsigned char*)&scale, sizeof(float));
    read_or_die(sp->c->fd, buf, buflen);
        
    if (&sp->s->left != sp->c) save_deltas(buf, sp->s->left.delta, valueslen, scale); 
    if (&sp->s->right != sp->c) save_deltas(buf, sp->s->right.delta, valueslen, scale);
    if (&sp->s->up != sp->c) save_deltas(buf, sp->s->up.delta, valueslen, scale);
    save_deltas(buf, sp->s->values, valueslen, scale);
    
  }
  return NULL;
}

void * synca(void* void_sp) {
  SyncParams* sp = (SyncParams*)void_sp;
  
  pthread_t in_thread;
  if(pthread_create(&in_thread, NULL, sync_in, void_sp)) {
    fprintf(stderr, "Error creating thread\n");
  }

  int valueslen = sp->s->valueslen;
  float* values = sp->c->delta;
  int buflen = (valueslen+7)/8;
  unsigned char buf[buflen];
  while (!sp->c->closing) {
    // figure out RMS magnitude
    // It's unclear the perfect scale number here
    // A smaller number would be good for getting
    // most of the values very accurate quickly.
    // A larger number would be best for getting all
    // the numbers closeish quickly.
        
    int i;
    float scale = 0;

    for (i=0; i<valueslen; i++)
      scale += sp->c->delta[i] * sp->c->delta[i];
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
    
    write_or_die(sp->c->fd, (unsigned char*)&scale, sizeof(float));
    write_or_die(sp->c->fd, buf, buflen);
    
  }
  
  if(pthread_join(in_thread, NULL)) {
    fprintf(stderr, "Error joining thread\n");
  }
  
  close(sp->c->fd);
  free((void*)sp);
  
  return NULL;
}


void * do_listening(void* void_s) {
   SharedTensor* s = (SharedTensor*)void_s;
   SyncParams* p_sp;
   
   struct sockaddr_in left_addr, right_addr, unused_addr;
   socklen_t unused_addrlen = sizeof(left_addr);
   
   
   // connect this guy to left side
   s->left.fd = accept_or_die(s->listen_fd, (struct sockaddr*)&left_addr, &unused_addrlen, &s->closing);
   if (s->closing) return NULL;
   write_or_die(s->left.fd,(unsigned char*)"Y",1);
   pthread_t left_thread=0;
   p_sp = (SyncParams*)malloc(sizeof(SyncParams));
   p_sp->s = s;
   p_sp->c = &s->left;
   if(pthread_create(&left_thread, NULL, synca, p_sp)) {
     fprintf(stderr, "Error creating thread\n");
   }
   
   // connect this guy to right side
   s->right.fd = accept_or_die(s->listen_fd, (struct sockaddr*)&right_addr, &unused_addrlen, &s->closing);
   if (s->closing) return NULL;
   write_or_die(s->right.fd,(unsigned char*)"Y",1);
   pthread_t right_thread=0;
   p_sp = (SyncParams*)malloc(sizeof(SyncParams));
   p_sp->s = s;
   p_sp->c = &s->right;
   if(pthread_create(&right_thread, NULL, synca, p_sp)) {
     fprintf(stderr, "Error creating thread\n");
   }
   
   // reject all future connections with redirects to children.
   int lrcounter=0;
   while(1) {
     int newfd = accept_or_die(s->listen_fd, (struct sockaddr*)&unused_addr, &unused_addrlen, &s->closing);
     if (s->closing) break;
     write_or_die(newfd,(unsigned char*)"N",1);
     struct sockaddr_in* addr_to_give = lrcounter++&1?&left_addr:&right_addr;
     write_or_die(newfd,(unsigned char*)addr_to_give,sizeof(left_addr));
     
     close(newfd);
   }
   if(pthread_join(left_thread, NULL)) {
     fprintf(stderr, "Error joining thread\n");
   }
   if(pthread_join(right_thread, NULL)) {
     fprintf(stderr, "Error joining thread\n");
   }
   return NULL;
}

int connect_to(SharedTensor* s, const char* host, const int port) {
   struct sockaddr_in serv_addr;
   socklen_t serv_addrlen = sizeof(serv_addr);
   struct hostent *server;
   
   int yes = 1;
   
   server = gethostbyname(host);
   if (server == NULL) { fprintf(stderr,"ERROR, no such host\n"); return -1; }
   
   bzero((unsigned char *) &serv_addr, sizeof(serv_addr));
   serv_addr.sin_family = AF_INET;
   bcopy((unsigned char *)server->h_addr, (unsigned char *)&serv_addr.sin_addr.s_addr, server->h_length);
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
     unsigned char buf;
     read_or_die(s->up.fd,&buf,1);
     
     if (buf=='Y') {
       pthread_t up_thread;
       SyncParams* upsp = (SyncParams*)malloc(sizeof(SyncParams));
       upsp->s = s;
       upsp->c = &s->up;
       if(pthread_create(&up_thread, NULL, synca, upsp)) {
         fprintf(stderr, "Error creating thread\n");
       }

       if (getsockname(s->up.fd, (struct sockaddr*)&serv_addr, &serv_addrlen)) {
         perror("ERROR finding own socket");
       };
       break;
     }
     
     // Weren't accepted, find who to connect to next lower down the tree
     read_or_die(s->up.fd, (unsigned char*)&serv_addr, sizeof(serv_addr));
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
  
  me->left.fd = -1;
  me->right.fd = -1;
  me->up.fd = -1;
  
  me->valueslen = THFloatTensor_nElement(input);
  me->up.delta = malloc(me->valueslen*sizeof(float));
  me->left.delta = malloc(me->valueslen*sizeof(float));
  me->right.delta = malloc(me->valueslen*sizeof(float));
  me->values = malloc(me->valueslen*sizeof(float));
  
  // Try to connect to given host/port
  if (connect_to(me, host, port)<0) {
    fprintf(stderr, "Connection error, exiting!\n");
    exit(1);
  }
  
  
  if (me->up.fd<0) {
    // we have no parent, so should add in the provided tensor.
    addFromInternal(me, input);
  } else {
    int i=0;    // Deliberately delay to get data delivered.
    // TODO:  use events properly
    while(!me->values[i++%me->valueslen]) {
      if (!i) sleep(1);
    }
  }
  
  return 1;
}


// defining functions callable from Lua
static int l_gc (lua_State *L) {
  SharedTensor *me = (SharedTensor *)lua_touserdata(L, 1);
  
  if ( me->left.fd == -1 && me->right.fd == -1 && me->up.fd == -1) {
    // nobody ever connected.
    close(me->listen_fd);
    
    me->closing = 1;
    
    struct sigaction actions;
    memset(&actions, 0, sizeof(actions));
    sigemptyset(&actions.sa_mask);
    actions.sa_flags = 0;
    actions.sa_handler = sighand;
    sigaction(SIGALRM,&actions,NULL);
    pthread_kill(me->listen_thread, SIGALRM);
    
    // rejoin tree of threads
    if(pthread_join(me->listen_thread, NULL)) {
       fprintf(stderr, "Error joining thread\n");
    }
    
    free(me->up.delta);
    free(me->left.delta);
    free(me->right.delta);
    free(me->values);
  } else {
    if ( me->up.fd != -1 ) {
      // TODO:  Fix this mess.  Currently, this object is un-gc-able.
      fprintf(stderr, "You tried to destroy a sharedtensor.  Due to my foolish coding, there isn't support for this client to tell all the other clients connected to it that they need to reconnect to other nodes.  If I disconnect now, they'll get all out of sync.  Hence, everyone on this sharedtensor will quit now to avoid inconsistency.  Patches welcome :-)\n");
      exit(-1);     
    } else {
      fprintf(stderr, "You tried to destroy the master sharedtensor. All clients will be disconnected and will terminate.\n");
      exit(-1);
    }
  }
  
  
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
