output host_ips {
  value = harvester_virtualmachine.host[*].network_interface[0].ip_address
}

output host_ids {
  value = harvester_virtualmachine.host.*.id
}

output worker_ips {
  value = harvester_virtualmachine.worker[*].network_interface[0].ip_address
}

output worker_ids {
  value = harvester_virtualmachine.worker.*.id
}
