# Interpret namespace and network name based on user name
locals {
  namespace = "${var.username}${var.namespace_ending}"
  network_name = "${var.username}${var.namespace_ending}/ds4eng"
}

data "harvester_image" "img" {
  display_name = var.img_display_name
  namespace    = "harvester-public"
}

data "harvester_ssh_key" "mysshkey" {
  name      = var.keyname
  namespace = local.namespace
}

resource "random_id" "secret" {
  byte_length = 5
}

resource "harvester_cloudinit_secret" "cloud-config" {
  name      = "cloud-config-${random_id.secret.hex}"
  namespace = local.namespace

  user_data = templatefile("cloud-init.tmpl.yml", {
      public_key_openssh = data.harvester_ssh_key.mysshkey.public_key
    })
}

resource "harvester_virtualmachine" "host" {
  
  count = var.host

  name                 = "${var.username}-host-${random_id.secret.hex}"
  namespace            = local.namespace
  restart_after_update = true

  description = "Base VM"

  cpu    = 2
  memory = "8Gi"

  efi         = true
  secure_boot = true

  run_strategy    = "RerunOnFailure"
  hostname        = "${var.username}-host-${random_id.secret.hex}"
  reserved_memory = "100Mi"
  machine_type    = "q35"

  network_interface {
    name           = "nic-1"
    wait_for_lease = true
    type           = "bridge"
    network_name   = local.network_name
  }

  disk {
    name       = "rootdisk"
    type       = "disk"
    size       = "52Gi"
    bus        = "virtio"
    boot_order = 1

    image       = data.harvester_image.img.id
    auto_delete = true
  }

  cloudinit {
    user_data_secret_name = harvester_cloudinit_secret.cloud-config.name
  }

  # Added timeouts!
  timeouts {
    create = "5m"
    update = "5m"
    delete = "5m"
  }

  # Example Instance labels
  # tags = {
  #  condenser_ingress_isEnabled = true
  #  condenser_ingress_isAllowed = true
  #  condenser_ingress_prometheus_hostname = "prometheus-${var.username}"
  #  condenser_ingress_prometheus_port = 9090
  #  condenser_ingress_streamlit_hostname = "protein-pipeline-${var.username}"
  #  condenser_ingress_streamlit_port = 8501
  # }
}

resource "harvester_virtualmachine" "worker" {
  
  count = var.worker

  name                 = "${var.username}-worker-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  namespace            = local.namespace
  restart_after_update = true

  description = "Base VM"

  cpu    = 5
  memory = "33Gi"

  efi         = true
  secure_boot = true

  run_strategy    = "RerunOnFailure"
  hostname        = "${var.username}-worker-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  reserved_memory = "100Mi"
  machine_type    = "q35"

  network_interface {
    name           = "nic-1"
    wait_for_lease = true
    type           = "bridge"
    network_name   = local.network_name
  }

  disk {
    name       = "rootdisk"
    type       = "disk"
    size       = "112Gi"
    bus        = "virtio"
    boot_order = 1

    image       = data.harvester_image.img.id
    auto_delete = true
  }

  cloudinit {
    user_data_secret_name = harvester_cloudinit_secret.cloud-config.name
  }
     
  # Added timeouts!
  timeouts {
    create = "5m"
    update = "5m"
    delete = "5m"
  }  
}