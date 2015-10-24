// build@ gcc -shared -I/home/sdonovan/lua/include -o mylib.so mylib.c
// includes for your code
#include <string.h>
#include <math.h>

// includes for Lua
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

// defining functions callable from Lua
static int l_createtable (lua_State *L) {
  int narr = luaL_optint(L,1,0);         // initial array slots, default 0
  int nrec = luaL_optint(L,2,0);   // intialof hash slots, default 0
  lua_createtable(L,narr,nrec);
  return 1;
}

static const luaL_reg mylib[] = {
    {"createtable",l_createtable},
    {NULL,NULL}
};

int luaopen_cool(lua_State *L)
{
    luaL_register (L, "sharedtensor", mylib);
    return 1;
}
