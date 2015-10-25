require 'torch'
require 'sharedtensor'

z = torch.range(2,5):float()
x = torch.range(1,4):float()

a = sharedtensor.createOrFetch("192.168.0.131", 50000, x)

print(z);

while true do
  a:copyToTensor(z);
  print(z)
  os.execute("sleep 1")
end
