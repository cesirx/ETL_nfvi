from math import ceil # Used to find the nearest integer that is greater than or equal to a given number
import re
from datetime import datetime, timezone

class VMdata:
    'Retrieve VM configuration data'


    def __init__(self, vm_obj):
        self.vm_obj = vm_obj

    def vmMOID_calculator(self):
        """Return the MOID of the Host."""

        return self.vm_obj._moId

    def timestamp_calculator(self):
        """Return current timestamp in ISO8601 format.""" 

        current_time = datetime.now(timezone.utc)

        return current_time.isoformat()
     
    def actualUsage_calculator(self):
        """Return the actual storage space (GB) consumed by a given VM."""

        #snap_re = re.compile('-[0-9]{6}')
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
    
    def UUID_calculator(self):
        """Return the UUID of a given VM."""

        return self.vm_obj.config.uuid

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
        """Return "YES" if VM is Realtime (GOLD ResourcePool). Else return "NO"."""

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
        """
        pattern_sriov = re.compile(r'(pciPassthru[0-9]{1,2})\.')      # Regex pattern matching SRIOV devices

        sriov_set = set()
        for option in self.vm_obj.config.extraConfig:
            pattern_match = pattern_sriov.search(option.key)
            if pattern_match:
                sriov_set.add(pattern_match.group(1))
        """
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
    
    def hostMOID_calculator(self):
        """Return the MOID of the Host in which this VM runs."""

        return self.vm_obj.runtime.host._moId

    def get_vnic_type(self, device):
        """Return vNIC info for the current VM."""
        
        vnic_type = ""
        if "VirtualSriovEthernetCard" in str(type(device)):
            vnic_type = "SR-IOV"
        elif "VirtualPCIPassthrough" in str(type(device)):
            vnic_type = "PCI-PT"
        elif "VirtualVmxnet3" in str(type(device)):
            vnic_type = "vmxnet3"
        elif "VirtualE1000" in str(type(device)):
            vnic_type = "e1000"

        return vnic_type

    def get_vnic_pmSessions(self, device):
        """Return Port Mirror sessions of current vnic... is any."""

        vnic_portKey = device.backing.port.portKey
        vnic_pmSession = ""
        analysed_DVS = []  
        for dpg in self.vm_obj.network:
            if 'DistributedVirtualPortgroup' in str(type(dpg)): # Only works for dVS objects
                if dpg.config.distributedVirtualSwitch.name not in analysed_DVS:
                    for pmSession in dpg.config.distributedVirtualSwitch.config.vspanSession:
                        if pmSession.enabled:
                            if (vnic_portKey in pmSession.sourcePortReceived.portKey) or (vnic_portKey in pmSession.sourcePortTransmitted.portKey):
                                vnic_pmSession = pmSession.name
                                break
                    analysed_DVS.append(dpg.config.distributedVirtualSwitch.name)

        return vnic_pmSession

    def get_dpg_name(self, portgroupKey):
        """Return dpg name."""
        
        dpg_name = ""
        for dpg in self.vm_obj.network:
            if 'DistributedVirtualPortgroup' in str(type(dpg)):
                if portgroupKey == dpg.key:
                    dpg_name = dpg.name
                    break
        
        return dpg_name

    def get_dpg_security(self, dpg_name):
        """Get DPG security parameters."""

        promiscuous = ""
        macChange = ""
        forged = ""
        for dpg in self.vm_obj.network:
            if dpg_name == dpg.name:
                promiscuous = dpg.config.defaultPortConfig.securityPolicy.allowPromiscuous.value
                macChange = dpg.config.defaultPortConfig.securityPolicy.macChanges.value
                forged = dpg.config.defaultPortConfig.securityPolicy.forgedTransmits.value
                break

        return promiscuous, macChange, forged

    def get_dpg_vlans(self, vnic_dpg_name):
        """Resturn list of vlans in a DPG."""

        vlan_list = []
        for dpg in self.vm_obj.network:
            if vnic_dpg_name == dpg.name:
                if "Range" in str(type(dpg.config.defaultPortConfig.vlan.vlanId)):
                    for item in dpg.config.defaultPortConfig.vlan.vlanId:
                        if item.start != item.end:
                            range_string = "{}-{}".format(item.start, item.end)
                        else:
                            range_string = item.start
                        vlan_list.append(range_string)
                        #vlan_list += list(range(item.start, item.end + 1, 1))
                elif "int" in str(type(dpg.config.defaultPortConfig.vlan.vlanId)):
                    vlan_list.append(dpg.config.defaultPortConfig.vlan.vlanId)

                break

        return vlan_list

    def get_dpg_active_uplinks(self, vnic_dpg_name, vnic_dvs_name):
        """Return active and standby uplinks in a DPG."""

        active_list = []
        standby_list = []
        uplink_dic = {} # {"Uplink": "vmnic"}
        for dvs in self.vm_obj.summary.runtime.host.config.network.proxySwitch:
            if dvs.dvsName == vnic_dvs_name:
                for uplink in dvs.uplinkPort:
                    for item in dvs.spec.backing.pnicSpec:
                        if uplink.key == item.uplinkPortKey:
                            uplink_dic[uplink.value] = item.pnicDevice       

        for dpg in self.vm_obj.network:
            if vnic_dpg_name == dpg.name:
                for uplink in dpg.config.defaultPortConfig.uplinkTeamingPolicy.uplinkPortOrder.activeUplinkPort:
                    if uplink in uplink_dic.keys():
                        active_list.append(uplink_dic[uplink])
                for uplink in dpg.config.defaultPortConfig.uplinkTeamingPolicy.uplinkPortOrder.standbyUplinkPort:
                    if uplink in uplink_dic.keys():
                        standby_list.append(uplink_dic[uplink])
                break

        return active_list, standby_list

    def get_dvs_name(self, vnic_dpg_name):
        """Return dvs name."""

        dvs_name = ""
        for dpg in self.vm_obj.network:
            if vnic_dpg_name == dpg.name:
                dvs_name = dpg.config.distributedVirtualSwitch.name
                break

        return dvs_name

    def get_dvs_lldp(self, vnic_dpg_name):
        """Return dVS LLDP status."""

        lldp = "false"
        for dpg in self.vm_obj.network:
            if vnic_dpg_name == dpg.name:
                if "lldp" in dpg.config.distributedVirtualSwitch.config.linkDiscoveryProtocolConfig.protocol:
                    lldp = "True"
                break

        return lldp
    
    def get_dpg_lb_policy(self, vnic_dpg_name):
        """Return DPG Loadbalancing Policy."""

        #Dictionary of possible loadbalancing models
        dpg_lb = {'loadbalance_ip': 'Route based on IP hash', 
            'loadbalance_srcmac': 'Route based on source MAC hash', 
            'loadbalance_srcid': 'Route based on originating virtual port', 
            'failover_explicit': 'Use explicit failover order', 
            'loadbalance_loadbased': 'Route based on physical NIC load'}

        policy = ""
        for dpg in self.vm_obj.network:
            if vnic_dpg_name == dpg.name:
                policy = dpg_lb[dpg.config.defaultPortConfig.uplinkTeamingPolicy.policy.value]

        return policy
    
    def pcislot_order(self, df_v_network):
        """Calculate PCI Slot order as presented to the GuestOS."""

        if self.vm_obj.runtime.powerState == 'poweredOn':
            df_v_network['temp_pci_order'] = ""
            #df_v_network['vNIC_pciSlotNumber'] = df_v_network['vNIC_pciSlotNumber'].astype(int, errors = 'ignore')
            for index, row in df_v_network.iterrows():
                if df_v_network.at[index, 'vNIC_pciSlotNumber'] != '':
                    slot = df_v_network.at[index, 'vNIC_pciSlotNumber']
                    slot_bin = bin(slot)[2:].zfill(12) # Slot to binary and add leading zeros to get a uniform length binary number
                    domain_bin = slot_bin[-5:]
                    bus_bin = slot_bin[-10:-5]
                    function_bin = slot_bin[-12:-10]

                    pciBridge =  int(bus_bin, 2) - 1    # According to VMware docs is the formula to identify VM pciBridge
                    pciBridgeSlot = ""
                    
                    #for opts in self.vm_obj.config.extraConfig:
                    #    if opts.key == f'pciBridge{pciBridge}.pciSlotNumber':
                    #        pciBridgeSlot = (opts.value)
                    #        break
                    
                    #print(pciBridge)
                    #print(pciBridgeSlot)

                    # vNIC order wil be determined by their connected pciBridge (the lower the bridge number the higher in the order list) and by their function (does not apply to e1000)
                    # To take into account both factors, we will store in float format pciBridge.function in the "pci_order" variable (ie, 4.1, 5.2...)
                    # "pci_order" will be stored in a new column that will be used to sort the DF
                    if row.vNIC_Type == 'e1000':
                        #pci_order = int(pciBridgeSlot) + int(slot)*0.01 ---> Original working
                        pci_order = int(pciBridge) + int(slot)*0.01
                    else:
                        #pci_order = int(pciBridgeSlot) + int(function_bin, 2)*0.1 ---> Original working
                        pci_order = int(pciBridge) + int(function_bin, 2)*0.1
                        
                    df_v_network.at[index, 'temp_pci_order'] = pci_order
            if len(df_v_network[df_v_network['vNIC_pciSlotNumber'] == ''].index) == 0:  # There are no empty cells in column "vNIC_pciSlotNumber". If there are, ordering fails
                df_v_network = df_v_network.sort_values(by=['temp_pci_order'])
                df_v_network = df_v_network.reset_index(drop = True)
                df_v_network['vNIC_GuestOS_Mapping_Order'] = df_v_network.index + 1

            df_v_network = df_v_network.drop(columns=['temp_pci_order'])
            
        else:
            df_v_network['vNIC_GuestOS_Mapping_Order'] = 'poweredOff'

        return df_v_network

