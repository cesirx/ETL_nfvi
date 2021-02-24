import re, paramiko, getpass, json, requests, urllib3
from datetime import datetime, timezone

urllib3.disable_warnings()  # To disable HTTPS security warnings when cert validation is disabled
class HostData:
    'Retrieve Host configuration data'

    def __init__(self, host_obj, df_vms):
        self.host_obj = host_obj
        self.df_vms = df_vms

    def modelInfo_calculator(self):
        """Return Host model."""

        return self.host_obj.hardware.systemInfo.model
    
    def hostMOID_calculator(self):
        """Return the MOID of the Host."""

        return self.host_obj._moId

    def clustername_calculator(self):
        """Return the name of the Cluster to which the host belongs."""

        cluster_name = self.host_obj.parent.name

        return cluster_name

    def esxiVersion(self):
        """Return version and Build of a ESXi."""

        return self.host_obj.config.product.version, self.host_obj.config.product.build

    def biosVersion(self):
        """Return BIOS version."""

        return self.host_obj.hardware.biosInfo.biosVersion

    def hypReservedCores_calculator(self):
        """Return Hypervisor reserved pCPUs."""
        # 10% of host compute resources are reserved by the Hypervisor
        
        return round(self.host_obj.summary.hardware.numCpuCores * 0.1)

    def hypReservedMEM_calculator(self):
        """Return Hypervisor reserved MEM."""
        # 10% of host compute resources are reserved by the Hypervisor
        
        return round(self.host_obj.summary.hardware.memorySize/1024**3 * 0.1)

    def realtimevCPUs(self):
        """Calculate the amount and % (of host total pCPUs) of vCPUs provisioned in RealTime VMs in the host"""

        host_total_pcpus = self.host_obj.summary.hardware.numCpuThreads  # If SMT is active pCPU means Thread/lCPU. If SMT is disabled pCPU means Core
      
        if not self.df_vms.empty:
            vcpus_realtime_regular_vms = self.df_vms[(self.df_vms['VM_RealTime'] == 'YES') & (self.df_vms['VM_LatencySensitivity'] == 'normal')]['VM_vCPU'].sum()
            vcpus_realtime_ls_vms = self.df_vms[(self.df_vms['VM_RealTime'] == 'YES') & (self.df_vms['VM_LatencySensitivity'] == 'high')]['VM_vCPU'].sum() * 2  # vCPUs of LS=high VMs run isolated in one core. Both pCPUs of the core are blocked for a vCPU (they count double)

            realtimeSum = vcpus_realtime_regular_vms + vcpus_realtime_ls_vms
            realtimeSum_percentage = round(realtimeSum*100/host_total_pcpus)
        else:
            realtimeSum = 0
            realtimeSum_percentage = 0
        
        return realtimeSum, realtimeSum_percentage

    def socketProvisionedvCPUs(self, socket):
        """Calculate the amount and % (of Socket total pCPUs) of vCPUs provisioned in a Socket"""

        socket_total_pcpus = round(self.host_obj.summary.hardware.numCpuThreads / self.host_obj.summary.hardware.numCpuPkgs)  # If SMT is active pCPU means Thread/LCPU. If SMT is disabled pCPU means Core

        if not self.df_vms.empty:
            vcpus_socket_regular_vms = self.df_vms[(self.df_vms['VM_NUMA'] == str(socket)) & (self.df_vms['VM_LatencySensitivity'] == 'normal')]['VM_vCPU'].sum()
            vcpus_socket_ls_vms = self.df_vms[(self.df_vms['VM_NUMA'] == str(socket)) & (self.df_vms['VM_LatencySensitivity'] == 'high')]['VM_vCPU'].sum()
            socket_provisioned_vcpus = vcpus_socket_regular_vms + vcpus_socket_ls_vms*2    # LS vCPUs count double as LS provides core isolation
            current_socket_occupation_percentage = round(socket_provisioned_vcpus*100/socket_total_pcpus)
        else:
            socket_provisioned_vcpus = 0
            current_socket_occupation_percentage = 0

        return socket_provisioned_vcpus, current_socket_occupation_percentage

    def provisionedvCPUs(self):
        """Calculate the amount and % (of host total pCPUs) of vCPUs provisioned (and hypervisor reserved) in the host"""

        host_total_pcpus = self.host_obj.summary.hardware.numCpuThreads  # If SMT is active pCPU means Thread/LCPU. If SMT is disabled pCPU means Core

        if not self.df_vms.empty:
            vcpus_regular_vms = self.df_vms[self.df_vms['VM_LatencySensitivity'] == 'normal']['VM_vCPU'].sum()
            vcpus_ls_vms = self.df_vms[self.df_vms['VM_LatencySensitivity'] == 'high']['VM_vCPU'].sum()
            host_provisioned_vcpus = vcpus_regular_vms + vcpus_ls_vms*2    # LS vCPUs count double as LS provides core isolation
            hypervisor_reserved_pcpus = round(host_total_pcpus * 0.1) # 10% of host pCPUs are reserved by the Hypervisor

            current_host_occupation = host_provisioned_vcpus + hypervisor_reserved_pcpus
            current_host_occupation_percentage = round(current_host_occupation*100/host_total_pcpus)
        else:
            host_provisioned_vcpus = 0
            current_host_occupation_percentage = 0

        return host_provisioned_vcpus, current_host_occupation_percentage

    def provisionedRAM(self):
        """Calculate the amount and % (of host total RAM) of RAM provisioned (and hypervisor reserved) in the host."""

        host_total_ram = round(self.host_obj.summary.hardware.memorySize/1024**3) # Bytes to GBytes

        hypervisor_reserved_RAM_GB = round(host_total_ram * 0.1/1024)    # Around 10% of host RAM is reserved by the Hypervisor
        if not self.df_vms.empty:
            total_provisioned_RAM_GB = self.df_vms['VM_vMEM_GB'].sum()

            current_host_occupation = total_provisioned_RAM_GB + hypervisor_reserved_RAM_GB
            current_host_occupation_percentage = round(current_host_occupation*100/host_total_ram)
        else:
            total_provisioned_RAM_GB = 0
            current_host_occupation_percentage = 0

        return total_provisioned_RAM_GB, current_host_occupation_percentage

    def socketProvisionedRAM(self, socket):
        """Calculate the amount and % (of host total RAM) of RAM provisioned in each Socket."""

        host_total_ram_GB = round(self.host_obj.summary.hardware.memorySize/1024**3) # Bytes to GBytes

        socket_mem_GB = int(host_total_ram_GB / self.host_obj.hardware.numaInfo.numNodes)
        if not self.df_vms.empty:
            socket_provisioned_vmem = self.df_vms[self.df_vms['VM_NUMA'] == str(socket)]['VM_vMEM_GB'].sum()
            current_socket_occupation_percentage = round(socket_provisioned_vmem*100/socket_mem_GB)
        else:
            socket_provisioned_vmem = 0
            current_socket_occupation_percentage = 0

        return socket_provisioned_vmem, current_socket_occupation_percentage

    def cpuOccupationRatio(self):
        """Calculate max host occupation ratio according to host type."""

        oversubscription_factor = {"A":1, "B":3, "C":3, "D":3, "F":1}     # Host oversubscription ratio depends on Cluster type (A, B, C or D). Type "F" hosts are not considered
        pattern_cl = re.compile(r'CL_[A-Z]*_([A-Z]{1})_.*')      # Regex pattern matching cluster naming
        pattern_match = pattern_cl.search(self.host_obj.parent.name)
        if pattern_match:
            factor = oversubscription_factor[pattern_match.group(1)] * 100
        else:
            factor = 0

        return factor

    def cpuRealTimeOccupationRatio(self):
        """Calculate max host occupation in RealTime VMs."""

        # RealTime VMs factor
        if '_C_' in self.host_obj.parent.name:
            realtime_factor = 0   # No Realtime VMs allowed in type C hosts
        elif 'MADDV' in self.host_obj.parent.name or 'MADLB' in self.host_obj.parent.name:
            realtime_factor = 1   # Realtime VMs must not exceed 100% of host resources in PREPRO
        else:
            realtime_factor = 0.8   # Realtime VMs must not exceed 80% of host resources in PRO

        return round(realtime_factor * 100)

    def dsInfo_calculator(self):
        """Return information the datastore name of current Host."""
        
        datastore_name = ""
        datastore_capacity = 0
        datastore_free = 0
        ds_provisioned = 0
        ds_swap = 0
        actual_mixed_space_storage_GB = 0
        for datastore in self.host_obj.datastore:    # Sum up all vHDDs of the VM in the corresponding local or external datastore
            if datastore.summary.type == 'VMFS' and '_localDS' in datastore.name:
                datastore_name = datastore.name  # The premise is that all vHDDs of the VM will be provisioned in the same datastore
                datastore_capacity = round(datastore.summary.capacity/(1024**3))
                datastore_free = round(datastore.summary.freeSpace/(1024**3))
                if not self.df_vms.empty:
                    ds_provisioned = self.df_vms[self.df_vms['Datastore_Name'] == datastore_name]['VM_Provisioned_Storage_GB'].sum()
                    ds_swap = self.df_vms[self.df_vms['Datastore_Name'] == datastore_name]['VM_SwapFile_Size_GB'].sum()
                    
                actual_mixed_space_storage_GB = datastore_capacity - ds_provisioned - ds_swap
                break

        return datastore_name, datastore_capacity, datastore_free, ds_provisioned, ds_swap, actual_mixed_space_storage_GB

    def sriovVMs(self):
        """Return information about SRIOV VMs.
        
        Results
        -------
        total_sriov_VMs
            Number of VMs with SRIOV interfaces
        total_sriov_Ports
            Number of SRIOV(VF) vNICs configured
        """

        total_sriov_VMs = 0
        total_sriov_Ports = 0
        if not self.df_vms.empty:
            total_sriov_VMs = (self.df_vms['SRIOV_vNICs'] != 0).sum()
            total_sriov_Ports = self.df_vms['SRIOV_vNICs'].sum()

        return total_sriov_VMs, total_sriov_Ports

    def pciptVMs(self):
        """Return information about PCIPT VMs.
        
        Results
        -------
        total_sriov_VMs
            Number of VMs with PCIPT interfaces
        total_sriov_Ports
            Number of PCIPT vNICs configured
        """

        total_pcipt_VMs = 0
        total_pcipt_Ports = 0
        if not self.df_vms.empty:
            total_pcipt_VMs = (self.df_vms['PCIPT_vNICs'] != 0).sum()
            total_pcipt_Ports = self.df_vms['PCIPT_vNICs'].sum()

        return total_pcipt_VMs, total_pcipt_Ports

    def snapshotAllowed_calculator(self, mixedSpaceSize):
        """Return whether a Snapshot fits into the available Mixed Space of the host."""

        if not self.df_vms.empty:
            if self.df_vms[self.df_vms['VM_Snapshot'] == 'True'].size > 0:  # There is at least one Snapshot present in the same host. Our rule is max. one snap per host
                self.df_vms['Snapshot_Allowed'] = 'NO'  
            else:
                for row in self.df_vms.itertuples():
                    if row.VM_Space_In_Disk_GB > mixedSpaceSize:    # Current VM disk usage is bigger than the Mixed Space. It does not fit. 
                        self.df_vms.at[row.Index, 'Snapshot_Allowed'] =  'NO'
                    else:                                           # Current VM disk usage is smaller than the Mixed Space. It fits.              
                        self.df_vms.at[row.Index, 'Snapshot_Allowed'] = 'YES'

        return self.df_vms

    def restorationAllowed_calculator(self, mixedSpaceSize):
        """Return whether a Restoration fits into the available Mixed Space of the host."""

        if not self.df_vms.empty:
            current_snapshots_disk_usage = 0
            if self.df_vms[self.df_vms['VM_Snapshot'] == 'True'].size > 0:  # There is at least one Snapshot present in the same host. Our rule is max. one snap per host
                current_snapshots_disk_usage =  self.df_vms[self.df_vms['VM_Snapshot'] == 'True']['VM_Space_In_Disk_GB'].sum()
            
            for row in self.df_vms.itertuples():
                if row.VM_Space_In_Disk_GB + current_snapshots_disk_usage > mixedSpaceSize:
                    self.df_vms.at[row.Index, 'Restoration_Allowed'] =  'NO'
                else:
                    self.df_vms.at[row.Index, 'Restoration_Allowed'] = 'YES'

        return self.df_vms

    def standardpNIC_info(self, df_h_network):
        """Return information about dVS and SRIOV interfaces."""

        for pnic in self.host_obj.config.network.pnic:
            if "vmnic" in pnic.device:
                df_h_network = df_h_network.append({'Host_Name': self.host_obj.name.split('.')[0], 'MOID': self.host_obj._moId, 'vmnic_Name': pnic.device, 'vmnic_Driver': pnic.driver, \
                    'vmnic_MAC': pnic.mac, 'vmnic_Device': pnic.pci, 'vmnic_Link_Status': 'up' if pnic.linkSpeed else 'down', 'vmnic_Configured_Speed_Mbps': pnic.spec.linkSpeed.speedMb if pnic.spec.linkSpeed else 'Auto'}, ignore_index=True)

        return df_h_network

    def pciPassThroughNIC_info(self, df_h_network):
        """Return information about PCI-PT and SRIOV interfaces."""

        for pciDevice in self.host_obj.config.pciPassthruInfo:
            try:    # PCI devices which are not NICs do not have the following attributes
                if pciDevice.sriovActive:    # PCI Devices configured as SR-IOV
                    df_h_network.at[(df_h_network['vmnic_Device'] == pciDevice.id), 'vmnic_Type'] = "SR-IOV"
                    df_h_network.at[(df_h_network['vmnic_Device'] == pciDevice.id), 'vmnic_max_VFs'] = pciDevice.maxVirtualFunctionSupported   
                elif pciDevice.passthruEnabled and pciDevice.passthruActive and pciDevice.id == pciDevice.dependentDevice:  # PCI Devices configured as PCI-PT
                    pciBusDevice = pciDevice.id.split(".")[0]
                    pciFunction = pciDevice.id.split(".")[1]
                    pcivmnic = ''
                    siblingpNic =  [bdf for bdf in df_h_network.vmnic_Device if pciBusDevice in bdf]
                    if siblingpNic:
                        for sibling in siblingpNic:
                            siblingFunction = sibling.split(".")[1]
                            functionDrift = int(siblingFunction) - int(pciFunction) 
                            siblingName = df_h_network.loc[(df_h_network['vmnic_Device'] == sibling), 'vmnic_Name'].to_string(index=False)
                            siblingNumber = siblingName.replace('vmnic','')
                            pciNumber = int(siblingNumber) - functionDrift
                            pcivmnic = "vmnic" + str(pciNumber)
                            break
                    df_h_network = df_h_network.append({'Host_Name': self.host_obj.name.split('.')[0], 'MOID': self.host_obj._moId, 'vmnic_Name': pcivmnic, 'vmnic_Device': pciDevice.id, 'vmnic_Type': "PCI-PT"}, ignore_index=True)

                df_h_network.at[(df_h_network['vmnic_Device'] == pciDevice.id), 'vmnic_configured_VFs'] = pciDevice.numVirtualFunction  # These are the values used for the "max_vfs" vector
            except:
                pass

        return df_h_network

    def virtualSwitch_info(self, df_h_network):
        """Return information about pNICs in dVS and vSwitches in the Host."""

        #dvsNics = []
        try:
            for dvs in self.host_obj.config.network.proxySwitch:    # Find pNICs assigned to current host dVS
                dvsNics = [pnic.split("-")[2] for pnic in dvs.pnic]
                for nic in dvsNics: 
                    df_h_network.at[df_h_network['vmnic_Name']==nic, 'vmnic_Type'] = "dVS"
                    df_h_network.at[df_h_network['vmnic_Name']==nic, 'vmnic_virtualSwitch'] = dvs.dvsName

            for vswitch in self.host_obj.config.network.vswitch:
                switchNics = vswitch.spec.policy.nicTeaming.nicOrder.activeNic + vswitch.spec.policy.nicTeaming.nicOrder.standbyNic
                for nic in switchNics: 
                    df_h_network.at[df_h_network['vmnic_Name']==nic, 'vmnic_Type'] = "vSwitch"
                    df_h_network.at[df_h_network['vmnic_Name']==nic, 'vmnic_virtualSwitch'] = vswitch.name
        except:
            pass

        return df_h_network

    def pnicNuma_calculator(self, df_h_network):
        """Return the Numa Node to which a given pNIC belongs by using its Bus number.
        Bus numbers lower than 130 belong to NUMA 0. Bus numbers greater than 130 belong too NUMA 1.
        """

        df_h_network['vmnic_NUMA'] = df_h_network['vmnic_Device'].map(lambda x: '1' if int(x.split(":")[1],16)>130 else '0')
        
        return df_h_network

    def timestamp_calculator(self):
        """Return current timestamp in ISO8601 format.""" 

        current_time = datetime.now(timezone.utc)

        return current_time.isoformat()

    def pciDevice_Model(self, df_h_network):
        """Resturn the PCI card model of each vmnic."""

        for pciDevice in self.host_obj.hardware.pciDevice:  
            #df_h_network.at[(df_h_network['vmnic_Device'] == pciDevice.id), 'vmnic_Model'] = pciDevice.deviceName.replace(' ','_').replace('-','_')
            df_h_network.at[(df_h_network['vmnic_Device'] == pciDevice.id), 'vmnic_Model'] = pciDevice.deviceName

        return df_h_network

    def vectorVF_calculator(self, df_h_network):
        """Calculate VF vector according to the configured VFs in each PCI Device."""

        # The position of each vmnic in the VF vector is determined by its B:D:F position
        pattern = re.compile(r'0000:(..):..\.(.)')
        for index, row in df_h_network.iterrows():
            m = pattern.search(row['vmnic_Device'])
            if m:
                bus = int(m.group(1), 16)   # HEX to DEC
                function = int(m.group(2), 16)/10   # HEX to DEC. Divided by 10 so that device Function is added as a decimal number to the Bus. 
                df_h_network.at[index, 'order'] = bus + function    # Device Function must be taken into account to determine PCI Device order. It is added to the int64 Bus variable as a decimal value.

        df_h_network = df_h_network.sort_values('order')
        #print(df_h_network[['vmnic_Device', 'vmnic_Name', 'vmnic_Type', 'vmnic_configured_VFs', 'Host_calculated_VF_Vector']])
        vector = ['0' if row['vmnic_Type'] != 'SR-IOV' else row['vmnic_configured_VFs'] for index, row in df_h_network[df_h_network['vmnic_Driver'] == 'i40en'].iterrows()]
        string_vector = ','.join((str(v) for v in vector))
        trusted_vector = re.sub('[1-9][0-9]*', '1', string_vector)
        df_h_network = df_h_network.drop(columns=['order'])
        df_h_network['Host_calculated_VF_Vector'] = string_vector
        df_h_network['Host_calculated_Trusted_Vector'] = trusted_vector
        df_h_network = df_h_network.sort_index()    # Found some problems with the Apply function of the Styler if df is not sorted by index

        return df_h_network

    """
    def getGSW_info(df_h_network, gsw_name, gsw_username, gsw_password):
        #Retrieve physical port information for pNICs.

        jump_server = "10.30.190.207"
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(hostname=jump_server, username=gsw_username, password=gsw_password)
            stdin, stdout, stderr = client.exec_command('ls -l')
            print(stderr.read())
            lldp = stdout.read()
            print(lldp)
        except:
            print("Connection failure")

        return df_h_network

    def connect_to_GSW(self, df_h_network):
        #Connect to phyisical network switches.

        try:
            gsw_username = input('Enter GSW username: ')
            gsw_password = getpass.getpass(prompt='Enter GSW password: ')

            pop_name = self.host_obj.name.split('.')[0][-7:-2]
            gsw_range = ["1","2"]
            for i in gsw_range:
                gsw_name = "GSW" + pop_name.upper() + i
                print(gsw_name) 
                df_h_network = getGSW_info(df_h_network, gsw_name, gsw_username, gsw_password)         
        except:
            pass

        return df_h_network
    """

    def connect_to_esxi(self, df_h_network, df_h, df_vms_network, esxi_username, esxi_password):
        """Connect to ESXi to retrieve additional information."""

        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname=self.host_obj.name, username=esxi_username, password=esxi_password, timeout=10)
        except:
            print(f"{self.host_obj.name} ESXi connection failure")
        else:
            # Code to get PCIPT|SRIOV to vmnic mapping
            stdin, stdout, stderr = client.exec_command('vmkchdev -l | grep vmnic')
            pt = stdout.read().decode('ascii').strip("\n")
            #pattern_pcipt = re.compile('([0-9:.]*) [0-9:]* [a-z0-9:]* (?:passthru|vmkernel) (vmnic[0-9]*)')
            for pci_device in df_h_network['vmnic_Device']:
                pattern_pcipt = re.compile(f'({pci_device}) [0-9:]* [a-z0-9:]* (?:passthru|vmkernel) (vmnic[0-9]*)')
                p = pattern_pcipt.search(pt)
                if p:
                    df_h_network.at[(df_h_network['vmnic_Device'] == p.group(1)) & (df_h_network['vmnic_Name'] == ''),'vmnic_Name'] = p.group(2)
                    if not df_vms_network.empty:
                        df_vms_network.at[(df_vms_network['pNIC_PCI_Device'] == p.group(1)) & (df_vms_network['pNIC_inUse'] == ''), 'pNIC_inUse'] = p.group(2)
                    #print(df_vms_network[(df_vms_network['pNIC_PCI_Device'] == p.group(1))][['pNIC_PCI_Device', 'pNIC_inUse']])

            # Code to get current i40en VFs and Trusted vector in host
            pattern_vector = re.compile(r'max_vfs.* ([0-9,]+)')
            stdin, stdout, stderr = client.exec_command('esxcli system module parameters list -m i40en')
            vector = stdout.read().decode('ascii').strip("\n")
            m = pattern_vector.search(vector)
            if m:
                df_h_network['Host_current_VF_Vector'] = m.group(1)
            
            pattern_trusted = re.compile(r'trust_all_vfs.* ([0-9,]{2,})')
            n = pattern_trusted.search(vector)
            if n:
                df_h_network['Host_current_Trusted_Vector'] = n.group(1)

            # Code to match PCI Device B:D:F to vmnic name
            stdin, stdout, stderr = client.exec_command('lspci | grep vmnic')
            lspci = stdout.read().decode('ascii').strip("\n")
            for index, row in df_h_network.iterrows():
                vmnic_device = row['vmnic_Device']
                pattern_lspci = re.compile(f'{vmnic_device} .*\[(vmnic[0-9]*)\]')
                m = pattern_lspci.search(lspci)
                if m:
                    df_h_network.at[index, 'vmnic_Name'] = m.group(1)

            # Code to get vmnic driver and firmware version
            pattern_driverVersion = re.compile(r'  Version: ([0-9.]+)')
            pattern_firmwareVersion = re.compile(r'  Firmware.*: .* [0x]+.* ([0-9.]+)')
            for index, row in df_h_network[df_h_network['vmnic_Driver'] == 'i40en'].iterrows():
                vmnic_name = row['vmnic_Name']
                stdin, stdout, stderr = client.exec_command(f'esxcli network nic get -n {vmnic_name}')
                versions = stdout.read().decode('ascii').strip("\n")

                m = pattern_driverVersion.search(versions)
                if m:
                    df_h_network.at[index, 'vmnic_Driver_version']= m.group(1)

                n = pattern_firmwareVersion.search(versions)
                if n:
                    df_h_network.at[index, 'vmnic_Firmware_version']= n.group(1)

            # Code to get ISM VIC version
            stdin, stdout, stderr = client.exec_command('esxcli software vib list | grep ism')
            ism_version = stdout.read().decode('ascii').strip("\n")
            ism_version = re.sub(' +', ' ', ism_version)  # Replacing multiple consecutive spaces with only one
            if ism_version:
                    df_h['VIB_ISM_Version'] = ism_version.split(' ')[1]
            
            # Code to get current Team Uplink for each dVS vNIC and add it to the df_vms_network dataframe
            if not df_vms_network.empty:
                for vm in df_vms_network[df_vms_network['Host_Name']==self.host_obj.name.split('.')[0]]['VM_Name'].unique():
                    stdin, stdout, stderr = client.exec_command(f'esxcli network vm list | grep {vm} | awk \'{{print $1}}\'')
                    vm_world = stdout.read().decode('ascii').strip("\n")
                    if vm_world:
                        stdin, stdout, stderr = client.exec_command(f'esxcli network vm port list -w {vm_world}')
                        vnics = stdout.read().decode('ascii').strip("\n")
                        for mac in df_vms_network[df_vms_network['VM_Name']==vm]['vNIC_MAC']:
                            pattern_teamUplink = re.compile(rf' *Port ID: ([0-9]*)\n *vSwitch: [a-zA-Z0-9_-]*\n *Portgroup: [a-zA-Z0-9_-]*\n *DVPort ID: [0-9]*\n *MAC Address: ({mac})\n *IP Address: [0-9.]*\n *Team Uplink: (vmnic[0-9]*)')
                            #pattern_teamUplink = re.compile(rf' *MAC Address: ({mac})\n *IP Address: [0-9.]*\n *Team Uplink: (vmnic[0-9]*)')
                            m = pattern_teamUplink.search(vnics)
                            if m:
                                df_vms_network.at[df_vms_network['vNIC_MAC'] == m.group(2), 'pNIC_inUse'] = m.group(3)
                                stdin, stdout, stderr = client.exec_command(f'vsish -e  get /net/portsets/DvsPortset-0/ports/{m.group(1)}/vmxnet3/rxSummary')
                                portSummary = stdout.read().decode('ascii').strip("\n")
                                pattern_ringSize = re.compile('1st ring size:([0-9]*)')
                                pattern_ringFull = re.compile('# of times the 1st ring is full:([0-9]*)')
                                r = pattern_ringSize.search(portSummary)
                                if r:
                                    df_vms_network.at[df_vms_network['vNIC_MAC'] == m.group(2), 'vNIC_rxBuffer_Ring1_bytes'] = r.group(1)
                                s = pattern_ringFull.search(portSummary)
                                if s:
                                    df_vms_network.at[df_vms_network['vNIC_MAC'] == m.group(2), 'vNIC_rxBuffer_Ring1_fullTimes'] = s.group(1)


        return df_h_network, df_h, df_vms_network

    def idrac_PCIeDeviceInfo(self, df_h_network, idrac_username, idrac_password):
        """Collect iDRAC info."""

        idracName = self.host_obj.name.replace('hv','rs')
        try:
            #print("\n- WARNING, server PCIe Function URIs for iDRAC %s\n" % idracName)
            req = requests.get('https://%s/redfish/v1/Systems/System.Embedded.1' % (idracName), auth=(idrac_username, idrac_password), verify=False)
            statusCode = req.status_code
        except:
            print(f"{idracName} iDRAC connection failure")
        else:
            try:
                data = req.json()
                pcie_devices=[]
                for i in data[u'PCIeFunctions']:
                    for ii in i.items():
                        #print(ii[1])
                        pcie_devices.append(ii[1])
                for i in pcie_devices:
                    req = requests.get('https://%s%s' % (idracName, i), auth=(idrac_username, idrac_password), verify=False)
                    statusCode = req.status_code
                    data = req.json()
                    #message = "\n\n- Detailed information for URI \"%s\"\n\n" % i
                    #print(message)
                    nic = 'False'
                    pci_bdf_hex = ''
                    ethernet_port_slot = ''
                    for ii in data.items():
                        #device = "%s: %s" % (ii[0], ii[1])
                        #print(device)
                        if ii[0] == '@odata.id':
                            pci_bdf_dec = ii[1].split('/')[-1]
                            pci_b_dec = pci_bdf_dec.split('-')[0]
                            pci_b_hex = format(int(pci_b_dec), 'x').zfill(2)    # zfill() method to pad a string with zeros
                            pci_bdf_hex = re.sub(f'{pci_b_dec}-', f'{pci_b_hex}-', pci_bdf_dec).replace('-',':')
                            last_quote_index = pci_bdf_hex.rfind(":")   # get the index of the last occurrence of char : in str.
                            correct_pci_bdf_hex = pci_bdf_hex[:last_quote_index] + "0." + pci_bdf_hex[last_quote_index+1:]

                        if ii[0] == 'DeviceClass' and ii[1] == 'NetworkController':
                            nic = 'True'
                        if ii[0] == 'Description' and 'Ethernet' in ii[1]:
                            nic = 'True'
                        if ii[0] == 'Name' and 'Ethernet' in ii[1]:
                            nic = 'True'
                        if ii[0] == 'Description' and 'Network' in ii[1]:
                            nic = 'True'
                        if ii[0] == 'Name' and 'Network' in ii[1]:
                            nic = 'True'

                        if ii[0] == 'Oem':
                            try:
                                for iii in ii[1]['Dell']['DellPCIeFunction'].items():
                                    if iii[0] == '@odata.id':
                                        ethernet_port_slot = iii[1]
                            except:
                                pass
                    if nic == 'True':
                        full_pci_bdf = '0000:' + correct_pci_bdf_hex
                        df_h_network.at[(df_h_network['vmnic_Device'] == full_pci_bdf), 'iDRAC_NIC_Slot'] = ethernet_port_slot.split('/')[-1].split('-')[0]
                        df_h_network.at[(df_h_network['vmnic_Device'] == full_pci_bdf), 'iDRAC_EthernetPort_Slot'] = ethernet_port_slot.split('/')[-1]
                        #print(df_h_network[['iDRAC_NIC_Slot', 'iDRAC_EthernetPort_Slot']])
            except:
                print(f"Unexpected failure while retrieving PCIe devices data from {idracName} iDRAC.")

        return df_h_network

    def idrac_ethernetInterfaces(self, df_h_network, idrac_username, idrac_password):
        """Collect iDRAC info.
        
        Code adapted from: https://github.com/dell/iDRAC-Redfish-Scripting/blob/e54ae3e03bf96c4f1cee563f64e696dcd67a2769/Redfish%20Python/GetSystemHWInventoryREDFISH.py#L547

        
        """

        #idracName = self.host_obj.name.split('.')[0].replace('hv','rs')
        idracName = self.host_obj.name.replace('hv','rs')

        try:
            response = requests.get('https://%s/redfish/v1/Systems/System.Embedded.1/NetworkInterfaces' % idracName,verify=False,auth=(idrac_username, idrac_password))
            data = response.json()
        except:
            print(f"{idracName} iDRAC connection failure")
        else:
            try:
                #message = "\n---- Network Device Information ----"
                #print(message)
                network_URI_list = []
                for i in data['Members']:
                    network = i['@odata.id']
                    network_URI_list.append(network)

                #if network_URI_list == []:
                #    message = "\n- WARNING, no network information detected for system\n"
                #    print(message)
                    
                for i in network_URI_list:
                    #message = "\n- Network device details for %s -\n" % i.split("/")[-1]
                    #print(message)
                    i=i.replace("Interfaces","Adapters")
                    response = requests.get('https://%s%s' % (idracName, i),verify=False,auth=(idrac_username, idrac_password))
                    data = response.json()

                    for ii in data.items():
                        if ii[0] == 'NetworkPorts':
                            network_port_urls = []
                            url_port = ii[1]['@odata.id']
                            response = requests.get('https://%s%s' % (idracName, url_port),verify=False,auth=(idrac_username, idrac_password))
                            data = response.json()

                            port_uri_list = []
                            for i in data['Members']:
                                port_uri_list.append(i['@odata.id'])

                    for z in port_uri_list:
                        response = requests.get('https://%s%s' % (idracName, z),verify=False,auth=(idrac_username, idrac_password))
                        data = response.json()
                        mac = ''
                        slot = ''
                        #message = "\n- Network port details for %s -\n" % z.split("/")[-1]
                        #print(message)
                        for ii in data.items():

                            """
                            if ii[0] == '@odata.id' or ii[0] == '@odata.context' or ii[0] == 'Metrics' or ii[0] == 'Links' or ii[0] == '@odata.type':
                                pass
                            elif ii[0] == 'Oem':
                                try:
                                    for iii in ii[1]['Dell']['DellSwitchConnection'].items():
                                        if iii[0] == '@odata.context' or iii[0] == '@odata.type':
                                            pass
                                        else:
                                            message = "%s: %s" % (iii[0], iii[1])
                                            print(message)
                                except:
                                    pass
                            else:
                            """
                            #message = "%s: %s" % (ii[0], ii[1])
                            #print(message)
                            if ii[0] == "AssociatedNetworkAddresses":
                                mac = ii[1][0]
                            if ii[0] == "@odata.id":
                                slot = ii[1].split('/')[-1]

                        #This one works for R740
                        df_h_network.at[(df_h_network['iDRAC_EthernetPort_Slot'] == slot + '-1'), 'vmnic_MAC'] = mac.lower()   
                        
                        #This one works for R730
                        #df_h_network.at[(df_h_network['vmnic_MAC'] == mac.lower()), 'iDRAC_EthernetPort_Slot'] = slot + '-1'
                        #df_h_network.at[(df_h_network['vmnic_MAC'] == mac.lower()), 'iDRAC_NIC_Slot'] = slot.split('-')[0]
            except:
                print(f"Unexpected failure while retrieving Ethernet Interfaces data from {idracName} iDRAC.")


        return df_h_network

    def get_FW_inventory(self, df_h, idrac_username, idrac_password):
        
        idracName = self.host_obj.name.replace('hv','rs')

        try:
            #print('Starting Inventory Scan...')
            req = requests.get('https://%s/redfish/v1/UpdateService/FirmwareInventory' % (idracName), auth=(idrac_username, idrac_password), verify=False)
            statusCode = req.status_code
        except:
            print(f"{idracName} iDRAC connection failure")
        else:
            try:
                data = req.json()
                for i in data[u'Members']:
                    for ii in i.items():
                        if ii[0] == u'@odata.id':
                            req = requests.get('https://{}{}'.format(idracName, ii[1]), auth=(idrac_username, idrac_password), verify=False)
                            statusCode = req.status_code
                            data2 = req.json()
                            store = 'False'
                            column_name = ''
                            for iii in data2.items():
                                #message = "\n%s: %s" % (iii[0], iii[1])
                                #print(message)
                                if iii[0] == 'Name':
                                    if iii[1] == 'System CPLD':
                                        store = 'True'
                                        column_name = 'CPLD_Version'
                                        #print("Name {}".format(iii[1]))
                                    elif iii[1] == 'Lifecycle Controller':
                                        store = 'True'
                                        column_name = 'iDRAC_Version'
                                        #print("Name {}".format(iii[1]))
                                if iii[0] == 'Version' and store == 'True':
                                    #print("Version {}".format(iii[1]))
                                    df_h[column_name] = iii[1].strip()   # Removing spaces
            except:
                print(f"Unexpected failure while retrieving Inventory version data from {idracName} iDRAC.")

            
            # The code below does not work for all iDRACs (older ones fail loading the URL)
            """
            try:
                #print('Starting Inventory Scan...')
                #req = requests.get('https://%s/redfish/v1/UpdateService/FirmwareInventory?$expand=*($levels=1)' % (idracName), auth=(idrac_username, idrac_password), verify=False)
                statusCode = req.status_code
            except:
                print("iDRAC connection failure")
            else:
                data = req.json()

            for i in data[u'Members']:
                store = 'False'
                column_name = ''
                for ii in i.items():
                    
                    #if ii[0] == u'@odata.type':
                    #    #message = "\n%s: %s" % (ii[0], ii[1])
                    #    #print(message)
                    #    if ii[0] == 'Name':
                    #        element_name = ii[1]
                    #        #print(element_name)
                    #    if ii[0] == 'Version':
                    #        element_version = ii[1]
                    #        #print(element_version)
                    #    message = "\n"
                    #elif ii[0] == "Oem":
                    #    for iii in ii[1][u'Dell'][u'DellSoftwareInventory'].items():
                    #        message = "%s: %s" % (iii[0], iii[1])
                    #        #print(message)
                    #        message = "\n"

                    #else:
                    
                    #message = "%s: %s" % (ii[0], ii[1])
                    #print(message)
                    #message = "\n"
                    if ii[0] == 'Name':
                        if ii[1] == 'System CPLD':
                            store = 'True'
                            column_name = 'CPLD_Version'
                        elif ii[1] == 'Lifecycle Controller':
                            store = 'True'
                            column_name = 'iDRAC_Version'
                    if ii[0] == 'Version' and store == 'True':
                        df_h[column_name] = ii[1]     
            """

        return df_h

    def idrac_cgi(self, df_h, df_h_network, idrac_username, idrac_password):

        idracName = self.host_obj.name.split('.')[0].replace('hv','rs')
        
        login_data = '<LOGIN><REQ><USERNAME>{}</USERNAME><PASSWORD>{}</PASSWORD></REQ></LOGIN>'.format(idrac_username, idrac_password)
        login_header = "<?xml version='1.0'?>" + login_data
        login_req_uri = 'https://{}/cgi-bin/{}'.format(idracName, 'login')  # First we need to log in to get the auth token
        try:
            login_req = requests.get(login_req_uri, data = login_header, verify=False)
            req_status_code = login_req.status_code
            req_content = login_req.text
        except:
            print(f"{idracName} iDRAC connection failure")
        else:
            if req_status_code == 200:
                sid_pattern = re.compile('<SID>(.*)</SID>') 
                pattern_match = sid_pattern.search(req_content)
                if pattern_match:
                    sid = pattern_match.group(1)    # Token to run our command in the next GET request
                    #print(f"SID {sid}")

                cookie = {'Cookie':f'sid={sid}'}
                racadm_command = 'hwinventory'
                racadm_command_data = '<EXEC><REQ><CMDINPUT>racadm {}</CMDINPUT><MAXOUTPUTLEN>0x0fff</MAXOUTPUTLEN></REQ></EXEC>'.format(racadm_command)
                racadm_command_header = "<?xml version='1.0'?>" + racadm_command_data
                racadm_command_req_uri = 'https://{}/cgi-bin/{}'.format(idracName, 'exec')  # We run the command by passing the token as a Cookie
                try:
                    racadm_command_req = requests.get(racadm_command_req_uri, data = racadm_command_header, cookies = cookie, verify=False)
                    racadm_command_req_status_code = racadm_command_req.status_code
                    racadm_command_req_content = racadm_command_req.text
                except:
                    print(f"Error while retrieving {idracName} iDRAC hwinventory: status code {racadm_command_req_status_code}")
                else:
                    if racadm_command_req_status_code == 200:
                        nic_match = re.findall(r'(Device Type = NIC.*?-----)', racadm_command_req_content, re.DOTALL)   # Avoiding regex greddiness
                        for nic in nic_match:
                            pattern_bus = re.compile(r'BusNumber = (.*)')
                            m = pattern_bus.search(nic)
                            bus = m.group(1)
                            bus_hex = format(int(bus), 'x').zfill(2)    # zfill() method to pad a string with zeros

                            pattern_device = re.compile(r'DeviceNumber = (.*)')
                            m = pattern_device.search(nic)
                            device = m.group(1)
                            device_hex = format(int(device), 'x').zfill(2)    # zfill() method to pad a string with zeros

                            pattern_function = re.compile(r'FunctionNumber = (.*)')
                            m = pattern_function.search(nic)
                            function = m.group(1)
                            function_hex = format(int(function), 'x')

                            bdf_hex = '0000:' + bus_hex + ':' + device_hex + '.' + function_hex

                            pattern_mac = re.compile(r'CurrentMACAddress = (.*)')
                            m = pattern_mac.search(nic)
                            mac = m.group(1)
                            df_h_network.at[(df_h_network['vmnic_Device'] == bdf_hex), 'vmnic_MAC'] = mac.lower()


                            pattern_port_slot = re.compile(r'FQDD = (.*)')
                            m = pattern_port_slot.search(nic)
                            port_slot = m.group(1)
                            nic_slot = port_slot.split('-')[0]
                            df_h_network.at[(df_h_network['vmnic_Device'] == bdf_hex), 'iDRAC_NIC_Slot'] = nic_slot
                            df_h_network.at[(df_h_network['vmnic_Device'] == bdf_hex), 'iDRAC_EthernetPort_Slot'] = port_slot

                        version_match = re.findall(r'(\[InstanceID: System.Embedded.1\].*?-----)', racadm_command_req_content, re.DOTALL)

                        for version in version_match:
                            pattern_idrac = re.compile(r'LifecycleControllerVersion = (.*)')
                            m = pattern_idrac.search(version)
                            idrac_version = m.group(1)
                            df_h['iDRAC_Version'] = idrac_version.strip()   # Removing spaces

                            pattern_cpld = re.compile(r'CPLDVersion = (.*)')
                            m = pattern_cpld.search(version)
                            cpld_version = m.group(1)
                            df_h['CPLD_Version'] = cpld_version.strip() # Removing spaces

        return df_h, df_h_network