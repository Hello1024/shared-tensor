# shared-tensor
A distributed, shared tensor with high performance approximate updates for machine learning

## Example use:

    require 'sharedtensor'
    
    original_tensor = torch.Tensor(4,5,6,2)

    -- Tries to connect to a shared tensor.  If the connection
    -- succeeds, use that, else start a new shared tensor with
    -- values from "original_tensor".
    z = sharedtensor.createOrFetch("192.168.0.1", 9925, original_tensor)

    while true do
      z:copyToTensor(current_values)
      new_values = ...  -- do learning on this...
      z:addFromTensor(new_values.csub(curent_values));
    end


## Usage notes:

The transfer uses compression which assumes latency for approximate updates is most important.  It also assumes the magnitude of all values is similar.  The synchronisation is fully ascynchronous, and while it will always eventually converge over time, values may overshoot temporaraly.

All machines must be able to connect to one another.  They will form a tree formation internally.  Nodes will pass around other nodes IP addresses, so all must be on an IPv4 network with no NAT or port mappings etc.  Multiple tensors must use different ports.  


## TODO:

* Way to limit network bandwidth based on fixed bitrate or fixed quality setting.  Currently simply fills all bandwidth.

* Nicer handling of disconnections and errors in general.  Reconnection logic would be cool.

* Tree "design" to allow for variable latency links or network topologies where some systems can't see others.

* More complete examples, including `char-rnn`

* Diagrams in documentation.

* Support "table sync" so that a table of tensors all get synched, but tensors can have different magnitudes

* Try different compression methods in the real world

* build AWS image/integration.

* Do the actual delta compression in a cuda kernel.  Should save *lots* of CPU.

## Want to help out?

Pull requests accepted!
