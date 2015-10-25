require 'torch'
require 'sharedtensor'

x = torch.range(1,4):float()

-- On the host:
--   creates a shared tensor hosted at 127.0.0.1:50000
--
-- On other clients:
--   connects to 127.0.0.1:50000 and keeps up two-way
--   sync with the host and all other clients.
a = sharedtensor.createOrFetch("127.0.0.1", 50000, x)

while true do
  a:copyToTensor(x);
  
  -- do something computationally intensive with x
  results = torch.Tensor(x:size()):fill(1):float()
  
  -- Add our updates into a, which will be asynchronously propogated
  -- to all other connected programs.
  a:addFromTensor(results);
  
  print(x)
  os.execute("sleep 1")  -- just so you can see whats going on.
end
