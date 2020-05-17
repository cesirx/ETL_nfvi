from math import ceil # Used to find the nearest integer that is greater than or equal to a given number
import re
class VMdata:
    'Retrieve VM configuration data'


    def __init__(self, vm_obj):
        self.vm_obj = vm_obj

    
    def actualUsage_calculator(self):
        """Return the actual storage space (GB) consumed by a given VM."""

        actual_size = 0
        for file in self.vm_obj.layoutEx.file:
            if ('snapshot' not in file.name.lower()) and ('delta' not in file.name.lower() and not re.search('-[0-9]{6}',file.name.lower())): # Excluding snapshot files from actual disk usage to get an accurate VM storage consumption
                actual_size += file.size  

        return round(actual_size/(1024**3))

    def snapshot_calculator(self):
        """Return "True" if the VM has an snapshot, or "False" if not."""

        if not self.vm_obj.snapshot:
            snapshot = 'False'
        else:
            snapshot = 'True'

        return snapshot

    def hddCapacity_calculator(self):
        """Return aggregated provisioned capacity (GB) for a given VM."""

        vm_total_provisioned_storage = 0
        for device in self.vm_obj.config.hardware.device:
            if "hard disk" in device.deviceInfo.label.lower():
                vm_total_provisioned_storage += device.capacityInKB

        return round(vm_total_provisioned_storage/(1024**2))


    def hddNumber_calculator(self):
        """Return the number of vHDDs in a given VM."""

        num_disks = 0
        for device in self.vm_obj.config.hardware.device:
            if "hard disk" in device.deviceInfo.label.lower():
                num_disks += 1

        return num_disks

    def hostname_calculator(self):
        """Return the name of the host in which VM runs."""

        host_name = self.vm_obj.summary.runtime.host.name

        return host_name.split('.')[0]

    def dsFree_calculator(self):
        """Return the free capacity (GB) of the datastore in which VM vHDDs are stored."""

        for device in self.vm_obj.config.hardware.device: 
            if "hard disk" in device.deviceInfo.label.lower():
                datastore_free = round(device.backing.datastore.summary.freeSpace/(1024**3))
                break

        return datastore_free

    def dsCapacity_calculator(self):
        """Return total capacity (GB) of the datastore in which VM vHDDs are stored."""

        for device in self.vm_obj.config.hardware.device:
            if "hard disk" in device.deviceInfo.label.lower():
                datastore_capacity = round(device.backing.datastore.summary.capacity/(1024**3))
                break

        return datastore_capacity

    def dsName_calculator(self):
        """Return the datastore in which VM vHDDs are stored."""

        for device in self.vm_obj.config.hardware.device:    # Sum up all vHDDs of the VM in the corresponding local or external datastore
            if "hard disk" in device.deviceInfo.label.lower():
                datastore_name = device.backing.datastore.name  # The premise is that all VM vHDDs will be provisioned in the same datastore

        return datastore_name

    def swap_calculator(self):
        """Return swap file size of a given VM."""

        swap_file_size_GB = (self.vm_obj.config.hardware.memoryMB - self.vm_obj.config.memoryAllocation.reservation)/1024

        return round(swap_file_size_GB)

    def powerState_calculator(self):
        """Return the power state of the VM."""

        return self.vm_obj.runtime.powerState

    def clusterName_calculator(self):
        """Return VM cluster name."""

        return self.vm_obj.runtime.host.parent.name

    def antiAffinityRule_calculator(self):
        """Return Anti Affinity VMs for a given VM."""
   
        antiAffinity_list = []
        try:
            for rule in self.vm_obj.runtime.host.parent.configurationEx.rule:
                if rule.__class__.__name__ == "vim.cluster.AntiAffinityRuleSpec":
                    antiAffinity_list_temp = []
                    for vm in rule.vm:
                        antiAffinity_list_temp.append(vm.name)
                    if self.vm_obj.name in antiAffinity_list_temp:
                        antiAffinity_list_temp.remove(self.vm_obj.name) # Add every VM except myself
                        antiAffinity_list = antiAffinity_list_temp

                    #affinity_list = [vm.name for vm in rule.vm if vm.name != self.vm_obj.name] # Add every VM except itself
        except:
            antiAffinity_list = []

        return antiAffinity_list


    def affinityRule_calculator(self):
        """Return Affinity VMs for a given VM."""
   
        affinity_list = []
        try:
            for rule in self.vm_obj.runtime.host.parent.configurationEx.rule:
                if rule.__class__.__name__ == "vim.cluster.AffinityRuleSpec":
                    affinity_list_temp = []
                    for vm in rule.vm:
                        affinity_list_temp.append(vm.name)
                    if self.vm_obj.name in affinity_list_temp:
                        affinity_list_temp.remove(self.vm_obj.name) # Add every VM except myself
                        affinity_list = affinity_list_temp
        except:
            affinity_list = []

        return affinity_list

    def ruleCompliant_calculator(self):
        """Return whether the VM conflicts or not with its configured Affinity or antiAffinity rules."""
   
        rule_observed = "True"
        try:
            for rule in self.vm_obj.runtime.host.parent.configurationEx.rule:
                rule_observed = "True"
                rule_temp_dict = {}
                for vm in rule.vm:
                    rule_temp_dict[vm.name] = vm.runtime.host.name
 
                if self.vm_obj.name in rule_temp_dict.keys():
                    del rule_temp_dict[self.vm_obj.name]    # Add every VM except myself to the dictionary
                    if rule.__class__.__name__ == "vim.cluster.AffinityRuleSpec":
                        for key in rule_temp_dict:
                            if rule_temp_dict[key] != self.vm_obj.runtime.host.name: # If any VM in the Affinity rule is in a different host as this one...
                                rule_observed = "False"
                    elif rule.__class__.__name__ == "vim.cluster.AntiAffinityRuleSpec":
                        for key in rule_temp_dict:
                            if rule_temp_dict[key] == self.vm_obj.runtime.host.name: # If any VM in the antiAffinity rule is in the same host as this one...
                                rule_observed = "False"

        except:
            pass

        return rule_observed


    def realtime_calculator(self):
        """Return "YES" if VM is in GOLD ResourcePool. Else return "NO"."""

        try:
            resourcePool = self.vm_obj.resourcePool.name
            if "GOLD" in resourcePool:
                realtime = "YES"
                realtime = "YES"
            else:
                realtime = "NO"
        except:
            realtime = ""
            resourcePool = "Not in a Resource Pool"

        return realtime, resourcePool

    def latency_calculator(self):
        """Return Latency Sensitivity setting for a given VM."""

        return self.vm_obj.config.latencySensitivity.level

    def numaNode_calculator(self):
        """Return Latency Sensitivity setting for a given VM."""

        numa = ""
        for opts in self.vm_obj.config.extraConfig:
            if opts.key == 'numa.nodeAffinity':
                numa = (opts.value)

        return numa

    def corePerSocket_calculator(self):
        """Return corePerSocket setting for a given VM."""

        return self.vm_obj.config.hardware.numCoresPerSocket

    def vCPU_calculator(self):
        """Return vCPU setting for a given VM."""

        return self.vm_obj.config.hardware.numCPU

    def vMEM_calculator(self):
        """Return vMEM setting for a given VM."""

        return round(self.vm_obj.config.hardware.memoryMB/1024)

    def hypReservedCores_calculator(self):
        """Return Hypervisor reserved pCPUs."""
        # 10% of host compute resources are reserved by the Hypervisor
        
        return round(self.vm_obj.runtime.host.summary.hardware.numCpuCores * 0.1)

    def hypReservedMEM_calculator(self):
        """Return Hypervisor reserved MEM."""
        # 10% of host compute resources are reserved by the Hypervisor
        
        return round(self.vm_obj.runtime.host.summary.hardware.memorySize/1024**3 * 0.1)

    def serialPort_calculator(self):
        """Return Serial Port data for a given VM."""
        
        label = ''
        proxyURI = ''
        serviceURI = ''
        direction = ''

        for device in self.vm_obj.config.hardware.device:
            if "serial port" in device.deviceInfo.label.lower():
                try:
                    label = device.deviceInfo.label
                    proxyURI = device.backing.proxyURI
                    serviceURI = device.backing.serviceURI
                    direction = device.backing.direction
                except:
                    label = ''
                    proxyURI = 'Not a Network Serial Port'
                    serviceURI = ''
                    direction = ''

        return label, proxyURI, serviceURI, direction

    def reservations_calculator(self):
        """Return CPU and RAM reservations for current VM."""
        
        return self.vm_obj.summary.config.cpuReservation, round(self.vm_obj.summary.config.memoryReservation/1024)

    def hostPackageMHz_calculator(self):
        """Return CPU Package speed in the host"""

        return self.vm_obj.runtime.host.summary.hardware.cpuMhz

    def sriovVirtualInterfaces_calculator(self):
        """Return the amount of SRIOV vNICs in current VM."""

        sriov_vnics_count = 0
        for device in self.vm_obj.config.hardware.device:
            if "SR-IOV" in device.deviceInfo.label:
                sriov_vnics_count += 1

        return sriov_vnics_count

    def pciptVirtualInterfaces_calculator(self):
        """Return the amount of PCIPT vNICs in current VM."""
        
        pcipt_vnics_count = 0
        for device in self.vm_obj.config.hardware.device:
            if "PCI device" in device.deviceInfo.label:
                pcipt_vnics_count += 1

        return pcipt_vnics_count

    def vmxnet3VirtualInterfaces_calculator(self):
        """Return the amount of VMXNET3 vNICs in current VM."""

        pattern_vmxnet3 = re.compile(r'(ethernet[0-9]{1,2})\.')      # Regex pattern matching VMXNET3 devices

        vmxnet3_set = set()
        for option in self.vm_obj.config.extraConfig:
            pattern_match = pattern_vmxnet3.search(option.key)
            if pattern_match:
                vmxnet3_set.add(pattern_match.group(1))

        return len(vmxnet3_set)

    def virtualHardwareVersion_calculator(self):
        """Return Virtual Hardware version for current VM."""

        return self.vm_obj.config.version


