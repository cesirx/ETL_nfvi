import re, paramiko, getpass

class HostData:
    'Retrieve Host configuration data'

    def __init__(self, host_obj, df_vms):
        self.host_obj = host_obj
        self.df_vms = df_vms

    def modelInfo_calculator(self):
        """Return Host model."""

        return self.host_obj.hardware.systemInfo.model
    
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
            vcpus_socket_regular_vms = self.df_vms[(self.df_vms['VM_NumaNode'] == str(socket)) & (self.df_vms['VM_LatencySensitivity'] == 'normal')]['VM_vCPU'].sum()
            vcpus_socket_ls_vms = self.df_vms[(self.df_vms['VM_NumaNode'] == str(socket)) & (self.df_vms['VM_LatencySensitivity'] == 'high')]['VM_vCPU'].sum()
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
            socket_provisioned_vmem = self.df_vms[self.df_vms['VM_NumaNode'] == str(socket)]['VM_vMEM_GB'].sum()
            current_socket_occupation_percentage = round(socket_provisioned_vmem*100/socket_mem_GB)
        else:
            socket_provisioned_vmem = 0
            current_socket_occupation_percentage = 0

        return socket_provisioned_vmem, current_socket_occupation_percentage

    def cpuOccupationRatio(self):
        """Calculate max host occupation ratio according to host type."""

        oversubscription_factor = {"A":1, "B":3, "C":3, "D":3, "F":1}     # Host oversubscription ratio depends on Cluster type (A, B, C or D). Type "F" hosts are not considered
        pattern_cl = re.compile(r'CL_[A-Z]*_([A-Z]{1})_.*')      # Regex pattern matching InfraV cluster naming
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
                df_h_network = df_h_network.append({'Host_Name': self.host_obj.name.split('.')[0],'vmnic_Name': pnic.device, 'vmnic_Driver': pnic.driver, \
                    'vmnic_MAC': pnic.mac, 'vmnic_Device': pnic.pci, 'vmnic_Link': 'up' if pnic.spec.linkSpeed else 'down'}, ignore_index=True)

        return df_h_network

    def pciPassThroughNIC_info(self, df_h_network):
        """Return information about PCI-PT and SRIOV interfaces."""

        for pciDevice in self.host_obj.config.pciPassthruInfo:
            try:    # PCI devices which are not NICs do not have the following attributes
                if pciDevice.sriovEnabled and pciDevice.sriovActive:    # PCI Devices configured as SR-IOV
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
                    df_h_network = df_h_network.append({'Host_Name': self.host_obj.name.split('.')[0], 'vmnic_Name': pcivmnic, 'vmnic_Device': pciDevice.id, 'vmnic_Type': "PCI-PT"}, ignore_index=True)

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
        except:
            pass

        for vswitch in self.host_obj.config.network.vswitch:
            switchNics = vswitch.spec.policy.nicTeaming.nicOrder.activeNic + vswitch.spec.policy.nicTeaming.nicOrder.standbyNic
            for nic in switchNics: 
                df_h_network.at[df_h_network['vmnic_Name']==nic, 'vmnic_Type'] = "vSwitch"
                df_h_network.at[df_h_network['vmnic_Name']==nic, 'vmnic_virtualSwitch'] = vswitch.name

        return df_h_network

    def pnicNuma_calculator(self, df_h_network):
        """Return the Numa Node to which a given pNIC belongs by using its Bus number.
        Bus numbers lower than 130 belong to NUMA 0. Bus numbers greater than 130 belong too NUMA 1.
        """

        df_h_network['vmnic_NUMA'] = df_h_network['vmnic_Device'].map(lambda x: '1' if int(x.split(":")[1],16)>130 else '0')
        
        return df_h_network

    def pciDevice_Model(self, df_h_network):
        """Resturn the PCI card model of each vmnic."""

        for pciDevice in self.host_obj.hardware.pciDevice:  
            df_h_network.at[(df_h_network['vmnic_Device'] == pciDevice.id), 'vmnic_Model'] = pciDevice.deviceName.replace(' ','_')

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
        df_h_network = df_h_network.drop(columns=['order'])
        df_h_network['Host_calculated_VF_Vector'] = string_vector
        df_h_network = df_h_network.sort_index()    # Found some problems with the Apply function of the Styler if df is not sorted by index

        return df_h_network

    def connect_to_esxi(self, df_h_network, esxi_username, esxi_password):
        """Connect to ESXi to retrieve addition pNIC information."""

        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname=self.host_obj.name, username=esxi_username, password=esxi_password)

            # Code to get current SRIOV VFs vector in host
            pattern_vector = re.compile(r'max_vfs.* ([0-9,]+)')
            stdin, stdout, stderr = client.exec_command('esxcli system module parameters list -m i40en')
            vector = stdout.read().decode('ascii').strip("\n")
            m = pattern_vector.search(vector)
            if m:
                df_h_network['Host_current_VF_Vector'] = m.group(1)

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

        except:
            print("ESXi connection failure")

        return df_h_network



