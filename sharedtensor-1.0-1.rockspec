package = "sharedtensor"
 version = "1.0-1"
 source = {
    url = "git://github.com/Hello1024/shared-tensor",
    tag = "v1.0",
 }
 description = {
    summary = "A distributed, shared tensor with high performance approximate updates for machine learning.",
    detailed = [[
       A distributed, shared tensor with high performance approximate updates for machine learning
    ]],
    homepage = "http://github.com/Hello1024/shared-tensor",
    license = "None"
 }
 dependencies = {
    "lua >= 5.1",
 }
 build = {
    type = "builtin",
    modules = {
       sharedtensor = {
          sources = {"src/sharedtensor.c"},
          defines = {"MAX_DATES_PER_MEAL=50"},
          libraries = {"m"},
       }
    },
 }
