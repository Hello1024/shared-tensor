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
} SharedTensor;

typedef struct {  // Used to pack params for thread creation.
  SharedTensor* s;
  Connection* c;
} SyncParams;

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
  char buf[buflen];
  while (!sp->c->closing) {
    float scale;
    if (read(sp->c->fd, &scale, sizeof(float)) != sizeof(float)) { fprintf(stderr, "Short Read\n"); sp->c->closing=1; return; }
    if (read(sp->c->fd, buf, buflen) != buflen) { fprintf(stderr, "Short Read\n"); sp->c->closing=1; return;}
        
    if (&sp->s->left != sp->c) save_deltas(buf, sp->s->left.delta, valueslen, scale); 
    if (&sp->s->right != sp->c) save_deltas(buf, sp->s->right.delta, valueslen, scale);
    if (&sp->s->up != sp->c) save_deltas(buf, sp->s->up.delta, valueslen, scale);
    save_deltas(buf, sp->s->values, valueslen, scale);
    
  }

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
  char buf[buflen];
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
    
    if (write(sp->c->fd, &scale, sizeof(float)) != sizeof(float)) fprintf(stderr, "Short Write\n");;
    if (write(sp->c->fd, buf, buflen) != buflen) { fprintf(stderr, "Short Write\n"); sp->c->closing=1; }
    
  }
  
  if(pthread_join(in_thread, NULL)) {
    fprintf(stderr, "Error joining thread\n");
  }
  
  close(sp->c->fd);
  free((void*)sp);
}


void * do_listening(void* void_s) {
   SharedTensor* s = (SharedTensor*)void_s;
   SyncParams* p_sp;
   
   struct sockaddr_in left_addr, right_addr, unused_addr;
   socklen_t unused_addrlen = sizeof(left_addr);
   
   
   // connect this guy to left side
   s->left.fd = accept(s->listen_fd, (struct sockaddr*)&left_addr, &unused_addrlen);
   if (write(s->left.fd,"Y",1)!=1) fprintf(stderr, "Conn died\n");
   pthread_t left_thread=0;
   p_sp = (SyncParams*)malloc(sizeof(SyncParams));
   p_sp->s = s;
   p_sp->c = &s->left;
   if(pthread_create(&left_thread, NULL, synca, p_sp)) {
     fprintf(stderr, "Error creating thread\n");
   }
   
   // connect this guy to right side
   s->right.fd = accept(s->listen_fd, (struct sockaddr*)&right_addr, &unused_addrlen);
   if (write(s->right.fd,"Y",1)!=1) fprintf(stderr, "Conn died\n");
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
     int newfd = accept(s->listen_fd, (struct sockaddr*)&unused_addr, &unused_addrlen);
     if (newfd<0) break;
     if(write(newfd,"N",1)!=1) fprintf(stderr, "Conn died\n");
     struct sockaddr_in* addr_to_give = lrcounter++&1?&left_addr:&right_addr;
     if(write(newfd,(char*)addr_to_give,sizeof(left_addr))!=sizeof(left_addr)) fprintf(stderr, "Conn died\n");
     
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
       fprintf(stderr, "Becoming master.\n");
       close(s->up.fd);
       s->up.fd = -1;
       break;
     }

     // successfully connected, but do we need to move?
     char buf;
     if (read(s->up.fd,&buf,1)!=1) fprintf(stderr, "Conn died\n");
     
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
     if (read(s->up.fd, &serv_addr, sizeof(serv_addr)) != sizeof(serv_addr)) fprintf(stderr, "Conn died\n");;
     close(s->up.fd);
   } while (1);
   
   /* start listening at same endpoint */
   s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
   if (s->listen_fd < 0) { perror("socket");}
	 
   if (setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
       perror("setsockopt");
   }
	 if (bind(s->listen_fd, (struct sockaddr*)&serv_addr, serv_addrlen)) {
	   perror("bind");
     fprintf(stderr, "Likley you specified an address to connect to which is neither on this machine nor running a copy of sharedtensor.\n"); 
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
