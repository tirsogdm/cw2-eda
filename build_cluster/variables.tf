variable img_display_name {
  type = string
  default = "AlmaLinux-9-GenericCloud-9.6-20250522"
}

# your rancher username (e.g. ucbcdwb)
variable username {
  type = string
  default = "ucabtg2"
}

variable namespace_ending {
  type = string
  default = "-comp0235-ns"
}

# The name of your ssh key uploaded to rancher 
variable keyname {
  type = string
  default = "comp0235keydemo"
}

variable host {
  type    = number
  default = 1
}

variable worker {
  type    = number
  default = 4
}
