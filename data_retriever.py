"""
Script to get information from vSphere VMs and Hosts through vCenter API. (--help to get requested arguments)

Returns:
    - CSV file with the queried data.
    - HTML code with data structured in a sortable table

Tested in:
    vCenter 6.5u3

Requires (pip):
    pandas (for "daraframes")
    ninja2 (for "pandas.style")
    matplotlib (for "background_gradient")

By Cesar Ortega
"""

from pyVmomi import vim     # Module "pyVmomi" to connect to vSphere API
from pyVim.connect import SmartConnect, Disconnect
import ssl
import argparse
import getpass
import atexit   # Cleanup module
import time
import pandas as pd
import numpy as np
from VMdata import VMdata
from HostData import HostData
import os
import datetime
import re
import requests
import json
import sys

def parse_arguments():
    """Process input arguments."""

    parser = argparse.ArgumentParser()
    parser.add_argument('vcenter_ip', help='vCenter ip/fqdn')
    parser.add_argument('vcenter_user', help='vCenter login username')
    parser.add_argument('-t', help='type of vCenter object which data is to be retrieved. Options are vm|host|cluster|datacenter', choices=['vm', 'host', 'cluster', 'datacenter'], required=True)
    parser.add_argument('-n', help='name of vCenter object which data is to be retrieved', required=True)
    parser.add_argument('--gsw', help='connect to network Switches to retrieve port configuration values corresponding to host pNICs', action="store_true", required=False)
    parser.add_argument('--esxiuser', help='Username to connect to ESXi and retrieve enhanced configuration values corresponding to host pNICs', required=False)

    return parser.parse_args()

def connect(vcenter_ip, vcenter_user, vcenter_password):
    """Connect to vCenter and bypass SSL warnings.

    Parameters
    ----------
    vcenter_ip : string
        vCenter IP address or resolvable FQDN
    vcenter_user : string
        vCenter username (must have at least read privileges)
    vcenter_password : string
        vCenter password  

    Returns
    -------
    c
        vCenter Service Instance connection
    """

    s = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    s.verify_mode = ssl.CERT_NONE
    try:
        c = SmartConnect(host=vcenter_ip, user=vcenter_user, pwd=vcenter_password)
        #print ('Valid cert')
    except:
        c = SmartConnect(host=vcenter_ip, user=vcenter_user, pwd=vcenter_password, sslContext=s)
        #print ('Invalid or untrusted cert')
    return c

def get_obj(content, vimtype, name = None):
    """Return vCenter objects matching a given type.
    
    Parameters
    ----------
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter
    vimtype : string
       pyvmomi object type to look for: VMs "[vim.VirtualMachine]", hosts "[vim.ComputeResource]", clusters "[vim.ClusterComputeResource]", etc. (per object name can be obtained from vCenter MOB URL)

    Results
    -------
    list
        list of vCenter object matching the given type
    """

    #content.rootFolder  --> starting point to look into
    #[vim.VirtualMachine]  -->   object types to look for
    #recursive  -->  whether we should look into it recursively
    return [item for item in content.viewManager.CreateContainerView(content.rootFolder, [vimtype], recursive=True).view]

def vm_scavenger(vm_obj):
    """Collect configuration data from a given VM.

    Parameters
    ----------
    vm_obj : pyVmomi.VmomiSupport.vim.VirtualMachine
       pyvmomi VM object 
       
       types: VMs "[vim.VirtualMachine]", hosts "[vim.ComputeResource]", clusters "[vim.ClusterComputeResource]", etc. (per object name can be obtained from vCenter MOB URL)

    Results
    -------
    df_v
        Dataframe with data about this VM. One counter per column
    """

    print('-- Gathering information from VM {}... '.format(vm_obj.name))
    df_v = pd.DataFrame(columns=['VM_Name', 'Host_Name', 'VM_vCPU', 'VM_vMEM_GB', 'VM_Provisioned_Storage_GB', 'VM_Space_In_Disk_GB',
                                    'VM_RealTime', 'VM_LatencySensitivity', 'VM_CoresPerSocket', 'VM_NumaNode', 'CPU_Reservation_MHz', 'RAM_Reservation_GB',
                                    'SRIOV_vNICs', 'VMXNET3_vNICs', 'PCIPT_vNICs', 'VM_SwapFile_Size_GB', 'Cluster_Name', 'Datastore_Name', 'Datastore_Capacity_GB', 
                                    'Datastore_Free_GB', 'VM_Provisioned_vHDDs', 'VM_Snapshot', 'Restoration_Allowed', 'Snapshot_Allowed', 'VM_PowerState', 'VM_AntiAffinity', 
                                    'VM_Affinity', 'VM_AR_Rule_Compliant', 'VM_SP_Label', 'VM_SP_proxyURI', 'VM_SP_serviceURI', 'VM_SP_direction', 'Host_CPU_Package_MHz',
                                    'VirtualHardware_Version'])

    vm_instance = VMdata(vm_obj)   # Creating an instance of VMdata class

    df_v = df_v.append({'VM_Name': vm_obj.name}, ignore_index=True)
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Host_Name'] = vm_instance.hostname_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Cluster_Name'] = vm_instance.clusterName_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Datastore_Name'] = vm_instance.dsName_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Datastore_Capacity_GB'] = vm_instance.dsCapacity_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Datastore_Free_GB'] = vm_instance.dsFree_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Provisioned_vHDDs'] = vm_instance.hddNumber_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Provisioned_Storage_GB'] = vm_instance.hddCapacity_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SwapFile_Size_GB'] = vm_instance.swap_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Space_In_Disk_GB'] = vm_instance.actualUsage_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Snapshot'] = vm_instance.snapshot_calculator() 
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_PowerState'] = vm_instance.powerState_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_AntiAffinity'] = [vm_instance.antiAffinityRule_calculator()]   # Use [] to specify the list to be included in the cell
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Affinity'] = [vm_instance.affinityRule_calculator()]   # Use [] to specify the list to be included in the cell
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_AR_Rule_Compliant'] = vm_instance.ruleCompliant_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_RealTime'] , df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_ResourcePool'] = vm_instance.realtime_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_LatencySensitivity'] = vm_instance.latency_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_CoresPerSocket'] = vm_instance.corePerSocket_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_vCPU'] = vm_instance.vCPU_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_vMEM_GB'] = vm_instance.vMEM_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_NumaNode'] = vm_instance.numaNode_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_Label'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_proxyURI'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_serviceURI'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_direction'] = vm_instance.serialPort_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'CPU_Reservation_MHz'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'RAM_Reservation_GB'] = vm_instance.reservations_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Host_CPU_Package_MHz'] = vm_instance.hostPackageMHz_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'SRIOV_vNICs'] = vm_instance.sriovVirtualInterfaces_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VMXNET3_vNICs'] = vm_instance.vmxnet3VirtualInterfaces_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'PCIPT_vNICs'] = vm_instance.pciptVirtualInterfaces_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VirtualHardware_Version'] = vm_instance.virtualHardwareVersion_calculator()

    return df_v

def host_scavenger(host_obj, arg_gsw, esxi_username='', esxi_password=''):
    """Collect info about VMs running in a given Host.

    Parameters
    ----------
    host_obj : pyVmomi.VmomiSupport.vim.ComputeResource
       pyvmomi Host object 

    Returns
    -------
    df_vms
        Dataframe with data from all VMs in this Host. One VM per row
    df_h
        Dataframe with data about this Host. One counter per column
    df_h_network
        Dataframe with Host pNIC information. One pNIC per row
    """

    df_vms = pd.DataFrame(columns=['VM_Name'])
    df_h = pd.DataFrame(columns=['Host_Name', 'Cluster_Name', 'Provisioned_vCPUs', 'Provisioned_RAM', 'Datastore_Provisioned_GB', 'RealTime_vCPUs', 
                                'RealTime_Occupation_%', 'Total_CPU_Occupation_%', 'Total_RAM_Occupation_%', 'Max_RealTime_Occupation_%', 
                                'Max_OverProv_Ratio_%', 'Socket0_Pinned_vCPUs', 'Socket0_CPU_Occupation_%', 'Socket1_Pinned_vCPUs', 'Socket1_CPU_Occupation_%',
                                'Socket0_Pinned_vMEM', 'Socket0_RAM_Occupation_%', 'Socket1_Pinned_vMEM', 'Socket1_RAM_Occupation_%',
                                'SRIOV_VMs', 'SRIOV_VFs_Provisioned', 'PCIPT_VMs', 'PCIPT_Devices_Provisioned', 'Datastore_Name', 'Datastore_Capacity_GB', 
                                'Datastore_Free_GB', 'Datastore_ProvisionedSwap_GB', 'Datastore_MixedSpace_GB', 'ESXi_Version',
                                'ESXi_Build', 'BIOS_Version', 'ESXi_Rsv_Cores', 'ESXi_Rsv_RAM_GB', 'Model'])
    df_h_network = pd.DataFrame(columns=['Host_Name', 'vmnic_Name', 'vmnic_Model', 'vmnic_Driver', 'vmnic_Driver_version', 'vmnic_Firmware_version', 'vmnic_MAC', 'vmnic_Device', 'vmnic_Type', 'vmnic_Link', 'vmnic_NUMA', 
                                'vmnic_virtualSwitch', 'vmnic_max_VFs', 'vmnic_configured_VFs', 'Host_calculated_VF_Vector', 'Host_current_VF_Vector', 'physical_Switch_name', 'physical_Switch_port', 'physical_Switch_port_VLANs'])
    
    print('** Gathering information from VMs in Host {}... '.format(host_obj.name.split('.')[0]))
    refreshDatastore(host_obj)
    for vm_obj in host_obj.vm:
        df_vms = df_vms.append(vm_scavenger(vm_obj), ignore_index=True)   # Adding one by one each VM in the Host to the df_vms Dataframe

    host_instance = HostData(host_obj, df_vms)   # Creating an instance of HostData class

    df_h = df_h.append({'Host_Name': host_obj.name.split('.')[0]}, ignore_index=True)
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Cluster_Name'] = host_instance.clustername_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Version'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Build'], =host_instance.esxiVersion()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'BIOS_Version'] = host_instance.biosVersion()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Rsv_Cores'] = host_instance.hypReservedCores_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Rsv_RAM_GB'] = host_instance.hypReservedMEM_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'RealTime_vCPUs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'RealTime_Occupation_%'] = host_instance.realtimevCPUs()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Max_RealTime_Occupation_%'] = host_instance.cpuRealTimeOccupationRatio()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Provisioned_vCPUs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Total_CPU_Occupation_%'] = host_instance.provisionedvCPUs()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Max_OverProv_Ratio_%'] = host_instance.cpuOccupationRatio()
    for socket in range(host_obj.hardware.numaInfo.numNodes):
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_Pinned_vCPUs'], \
            df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_CPU_Occupation_%'] = host_instance.socketProvisionedvCPUs(socket)
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_Pinned_vMEM'], \
            df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_RAM_Occupation_%'] = host_instance.socketProvisionedRAM(socket)
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Model'] = host_instance.modelInfo_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Provisioned_RAM'],\
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Total_RAM_Occupation_%'] = host_instance.provisionedRAM()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Name'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Capacity_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Free_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Provisioned_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_ProvisionedSwap_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_MixedSpace_GB'] = host_instance.dsInfo_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'SRIOV_VMs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'SRIOV_VFs_Provisioned'] = host_instance.sriovVMs()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'PCIPT_VMs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'PCIPT_Devices_Provisioned'] = host_instance.pciptVMs()

    # The following two metrics can only be obtained here as at this point df_vms and df_h are complete
    df_vms = host_instance.snapshotAllowed_calculator(df_h['Datastore_MixedSpace_GB'].item())
    df_vms = host_instance.restorationAllowed_calculator(df_h['Datastore_MixedSpace_GB'].item())

    df_h_network = host_instance.standardpNIC_info(df_h_network)
    df_h_network = host_instance.pciPassThroughNIC_info(df_h_network)
    df_h_network = host_instance.virtualSwitch_info(df_h_network)
    df_h_network = host_instance.pnicNuma_calculator(df_h_network)
    df_h_network = host_instance.pciDevice_Model(df_h_network)
    if esxi_username and esxi_password:
        df_h_network = host_instance.connect_to_esxi(df_h_network, esxi_username, esxi_password)
    df_h_network = host_instance.vectorVF_calculator(df_h_network)

    #response = requests.get('https://localhost:444/redfish/v1/Systems/System.Embedded.1',verify=False,auth=("user", "pass"))
    #response = requests.get('https://localhost:444/redfish/v1/Systems/System.Embedded.1/NetworkInterfaces',verify=False,auth=("user", "pass"))
    #response = requests.get('https://localhost:444/redfish/v1/Systems/System.Embedded.1/NetworkInterfaces/NIC.Integrated.1/NetworkPorts',verify=False,auth=("user", "pass"))
    #data = response.json()
    #print(data)

    return df_vms, df_h, df_h_network

def cluster_scavenger(cluster_obj, arg_gsw, esxi_username='', esxi_password=''):
    """Iterate through Hosts in a given Cluster.

    Parameters
    ----------
    cluster_obj : pyVmomi.VmomiSupport.vim.ComputeResource
       pyvmomi Host object 

    Returns
    -------
    df_vms
        Dataframe with data from all VMs in this Cluster. One VM per row
    df_host
        Dataframe with data from all Hosts in this Cluster. One Host per row
    df_hosts_network
        Dataframe woth pNIC data from all Hosts in this Cluster. One pNIC per row
    df_c
        Dataframe with data about this Cluster. One counter per column
    """

    df_vms = pd.DataFrame(columns=['VM_Name'])
    df_hosts = pd.DataFrame(columns=['Host_Name'])
    df_hosts_network = pd.DataFrame(columns=['Host_Name'])
    df_c = pd.DataFrame(columns=['Cluster_Name'])

    print('## Gathering information from Hosts in Cluster {}... '.format(cluster_obj.name))
    for host_obj in cluster_obj.host:
        df_temp_v, df_temp_h, df_temp_h_network = host_scavenger(host_obj, arg_gsw, esxi_username, esxi_password)
        df_vms = df_vms.append(df_temp_v, ignore_index=True)   # Adding one by one the configuration data from each VM to the host Dataframe
        df_hosts = df_hosts.append(df_temp_h, ignore_index=True)
        df_hosts_network = df_hosts_network.append(df_temp_h_network, ignore_index=True)
    
    df_c = df_c.append({'Cluster_Name': cluster_obj.name}, ignore_index=True)

    return df_vms, df_hosts, df_hosts_network, df_c

def datacenter_scavenger(datacenter_obj, arg_gsw, esxi_username='', esxi_password=''):
    """Iterate through Clusters in a given Datacenter.

    Parameters
    ----------
    datacenter_obj : pyVmomi.VmomiSupport.vim.Datacenter
       pyvmomi Datacenter object 

    Returns
    -------
    df_vms
        Dataframe with data from all VMs in this Datacenter. One VM per row
    df_hosts
        Dataframe with data from all Hosts in this Datacenter. One Host per row
    df_clusters
        Dataframe with data from all Clusters in this Datacenter. One Host per row
    df_d
        Dataframe with data about this Datacenter. One counter per column
    """

    df_vms = pd.DataFrame(columns=['VM_Name'])
    df_hosts = pd.DataFrame(columns=['Host_Name'])
    df_hosts_network = pd.DataFrame(columns=['Host_Name'])
    df_clusters = pd.DataFrame(columns=['Cluster_Name'])
    df_d = pd.DataFrame(columns=['Datacenter_Name'])

    #df_cluster = pd.DataFrame(columns=['VM_Name', 'Host_name', 'Cluster_name', 'Datastore_Name', 'Datastore_Capacity_GB', 'Datastore_Free_GB', 'VM_Provisioned_vHDDs', 
    #                                'VM_Provisioned_Storage_GB', 'VM_SwapFile_Size_GB', 'VM_Space_In_Disk_GB', 'VM_Snapshot', 'VM_PowerState', 'VM_AntiAffinity', 'VM_Affinity'])

    print('// Gathering information from Clusters in Datacenter {}... '.format(datacenter_obj.name))
    for cluster_obj in datacenter_obj.hostFolder.childEntity:
        df_temp_v, df_temp_h, df_temp_h_network, df_temp_c = cluster_scavenger(cluster_obj, arg_gsw, esxi_username, esxi_password)
        df_vms = df_vms.append(df_temp_v, ignore_index=True)   # Adding one by one the configuration data from each VM to the VM Dataframe
        df_hosts = df_hosts.append(df_temp_h, ignore_index=True)    # Adding one by one the configuration data from each Host to the host Dataframe
        df_clusters = df_clusters.append(df_temp_c, ignore_index=True)  # Adding one by one the configuration data from each Cluster to the Cluster Dataframe
        df_hosts_network = df_hosts_network.append(df_temp_h_network, ignore_index=True)

    df_d = df_d.append({'Datacenter_Name': datacenter_obj.name}, ignore_index=True)

    return df_vms, df_hosts, df_hosts_network, df_clusters, df_d

def refreshDatastore(host_obj):
    """Refresh host Datastore Storage information."""

    for datastore_obj in host_obj.datastore:
        if datastore_obj.summary.type == "VMFS":
            datastore_obj.RefreshDatastoreStorageInfo() # Refresh Datastore capacity  

def findDatacenterObj(datacenter_string, content):
    """Get pyvmomi object corresponding to input Datacenter name.

    Parameters
    ----------
    datacenter_name : string
        vSphere Datacenter name to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    datacenter_obj
       pyvmomi Datacenter object 
    """

    datacenter = []
    for datacenter_obj in get_obj(content, vim.Datacenter):   # Create Datacenter view 
        if datacenter_string in datacenter_obj.name:  # Datacenter string found
            datacenter.append(datacenter_obj)

    if not datacenter:
        print()
        print("Cluster not found. Terminating program...")
        print()
        exit()

    return datacenter

def findClusterObj(cluster_name, content):
    """Get pyvmomi object corresponding to input Cluster name.

    Parameters
    ----------
    cluster_name : string
        vSphere Cluster to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    cluster_obj
       pyvmomi Cluster object 
    """

    cluster = ""
    for cluster_obj in get_obj(content, vim.ClusterComputeResource):   # Create Cluster view 
        if cluster_obj.name == cluster_name:  # Cluster found
            cluster = cluster_obj
            break

    if not cluster:
        print()
        print("Cluster not found. Terminating program...")
        print()
        exit()

    return cluster

def findHostObj(host_name, content):
    """Get pyvmomi object corresponding to input Host name.

    Parameters
    ----------
    host_name : string
        vSphere host to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    host_obj
       pyvmomi Host object 
    """

    host = ""
    for host_obj in get_obj(content, vim.HostSystem):   # Create Host view 
        if (host_obj.name.split('.')[0].lower() == host_name.lower()) or (host_obj.name.lower() == host_name.lower()):  # Host found
            host = host_obj
            break
    
    if not host:
        print()
        print("Host not found. Terminating program...")
        print()
        exit()

    return host

def findVMObj(vm_name, content):
    """Get pyvmomi object corresponding to input VM name.

    Parameters
    ----------
    vm_name : string
        vSphere VM to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    vm_obj
       pyvmomi VM object 
    """

    vm = ""
    for vm_obj in get_obj(content, vim.VirtualMachine):   # Create VM view 
        if vm_obj.name == vm_name:  # VM found
            vm = vm_obj
            break 

    if not vm:
        print()
        print("VM not found. Terminating program...")
        print()
        exit()

    return vm

def addStickyHeaderCSS(new_html):
    """Enhance HTML code with CSS to make table headers stick to the top upon scrolling.
    
    Parameters
    ----------
    new_html : string
        HTML code 
    Returns
    -------
    sticky_html
        Input HTML code with CSS code to make table headers sticky
    """
    
    # Add the above inside the <style></style>
    css_sticky_header_code = """
        th {
            position: -webkit-sticky;
            position: sticky;
            top: 0;
            z-index: 2;
            }
        """

    css_sticky_left_column_header_code = """
        th:first-child {
            position: -webkit-sticky;
            position: sticky;
            left: 0;
            z-index: 2;
            background: #ccc;
            }
        thead th:first-child {
            z-index: 5;
            }
    """

    css_sticky_left_column_code = """
        tbody tr td:nth-child(1) {  /*the first cell in each tr*/
		  position: -webkit-sticky;
		  position: sticky;
		  left: 0px;
		  z-index: 2
        }
    """

    #th_property = f'onclick="sortTable({i})"'
    sticky_html = re.sub(r'(</style>)',fr'{css_sticky_header_code}\n{css_sticky_left_column_header_code}\n{css_sticky_left_column_code}\n \1', new_html)

    return sticky_html

def addSortFunctionJs(html, tableName):
    """Enhance HTML code with JavaScript function to sort tables when header is clicked.
    
    Parameters
    ----------
    html : string
        HTML code as returned by Pandas.Style.render()
    Returns
    -------
    new_html
        Input HTML code with a sorting JS function and "onclick" data in each "th" Tag
    """

    tableID = tableName

    # This line should take tableID from the above variable______ REVIEW
    ## table = document.getElementById("T_{html_vm_tableID});
    javascript_sort_function = """
        <script>
            function sortTable(n) {
                var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById("T_myTable");
                switching = true;
                //Set the sorting direction to ascending:
                dir = "asc"; 
                /*Make a loop that will continue until
                no switching has been done:*/
                while (switching) {
                    //start by saying: no switching is done:
                    switching = false;
                    rows = table.rows;
                    /*Loop through all table rows (except the
                    first, which contains table headers):*/
                    for (i = 1; i < (rows.length - 1); i++) {
                        //start by saying there should be no switching:
                        shouldSwitch = false;
                        /*Get the two elements you want to compare,
                        one from current row and one from the next:*/
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];
                        var cmpX=isNaN(parseInt(x.innerHTML))?x.innerHTML.toLowerCase():parseInt(x.innerHTML);
                        var cmpY=isNaN(parseInt(y.innerHTML))?y.innerHTML.toLowerCase():parseInt(y.innerHTML);
                        cmpX=(cmpX=='-')?0:cmpX;
                        cmpY=(cmpY=='-')?0:cmpY;
                        /*check if the two rows should switch place,
                        based on the direction, asc or desc:*/
                        if (dir == "asc") {
                            if (cmpX > cmpY) {
                                //if so, mark as a switch and break the loop:
                                shouldSwitch= true;
                                break;
                            }
                        } else if (dir == "desc") {
                            if (cmpX < cmpY) {
                                //if so, mark as a switch and break the loop:
                                shouldSwitch = true;
                                break;
                            }
                        }
                    }
                    if (shouldSwitch) {
                        /*If a switch has been marked, make the switch
                        and mark that a switch has been done:*/
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        //Each time a switch is done, increase this count by 1:
                        switchcount ++;      
                    } else {
                        /*If no switching has been done AND the direction is "asc",
                        set the direction to "desc" and run the while loop again.*/
                        if (switchcount == 0 && dir == "asc") {
                            dir = "desc";
                            switching = true;
                        }
                    }
                }
            }
        </script>
    """

    html = html.replace('</th>','</th>\n')

    occurences = len(re.findall(r'(<th) (class="col)',html)) # Number of "<th class="col" strings in the HTML code
    # For every column/header in the table, replace "<th class=" with "<th onclick="sortTable(i)" class="
    for i in range(occurences):
        th_property = f'onclick="sortTable({i})"'
        html = re.sub(r'(<th) (class="col)',fr'\1 {th_property} \2', html, 1)

    new_html = html + '\n' + javascript_sort_function

    new_html = addStickyHeaderCSS(new_html)

    return new_html

def writeOuputDataframes(queryObject, queryName, df_vms=pd.DataFrame(), df_hosts=pd.DataFrame(), df_hosts_network=pd.DataFrame(), df_clusters=pd.DataFrame(), df_datacenters=pd.DataFrame()):
    """Write output dataframes to HTML and CSV files.
    
    Parameters
    ----------
    df_vms : Dataframe (optional)
        Dataframe with data from all analyzed VMs. One VM per row
    df_host : Dataframe (optional)
        Dataframe with data from all analyzed Hosts. One Host per row
    df_hosts_network: Dataframe (optional)
        Dataframe woth pNIC data from all Hosts in this Cluster. One pNIC per row
    df_clusters : Dataframe (optional)
        Dataframe with data about all analyzed Clusters. One Cluster per row
    df_datacenters : Dataframe (optional)
        Dataframe with data about all analyzed Datacenters. One Datacenter per row
    queryObject : string
        vCenter object under analysis as per received arguments
    queryName : string
        vCenter object number under analysis as per received arguments
    """

    html_tableID = 'myTable'

    current_time = datetime.datetime.now()
    time_suffix = str(current_time.year) + str(current_time.month) + str(current_time.day) + str(current_time.hour) + str(current_time.minute)

    df_vms.fillna('',inplace=True)
    if not df_vms.empty:
        df_vms = df_vms.infer_objects() # Automatically convert each DF column to the appropiate type
        html_vms = (df_vms.style.hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', '#4CAF50'),('color', 'white'),('padding', '5px'),('font-size', '10pt'), ('cursor', 'pointer')]},
                                                    #{'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f2f2f2')]},
                                                    #{'selector': 'tr:nth-child(odd)', 'props': [('background-color', 'lightgray')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'yellow')]},
                                                    {'selector': 'tr', 'props': [('font-size', '10pt'),('background-color', 'white')]}
                                                    ])                                 
                                .set_properties(**{'text-align': 'right', 'border-color': 'black', 'border-style': 'solid', 'border-width': '1px'})  # Set some table properties
                                #.highlight_max(color='orange')
                                .apply(lambda x: ["background-color: silver" for index, value in enumerate(x)], axis = 0, subset=['VM_Name'])
                                #.apply(lambda x: ["color: white" for index, value in enumerate(x)], axis = 0, subset=['VM_Name'])
                                .bar(subset=['VM_vCPU'], color='#08D8C3')
                                .bar(subset=['VM_vMEM_GB'], color='lightgreen')
                                .bar(subset=['VM_Provisioned_Storage_GB'], color='#0855D8')
                                .bar(subset=['VM_Space_In_Disk_GB'], color='#FFD700')
                                .bar(subset=['SRIOV_vNICs'], color='#D1D86C')
                                .bar(subset=['VMXNET3_vNICs'], color='moccasin')
                                .bar(subset=['PCIPT_vNICs'], color='aquamarine')
                                .apply(lambda x: ["background-color: red" if (value.lower() != 'true') else "" for index, value in enumerate(x)], axis = 0, subset=['VM_AR_Rule_Compliant']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value.lower() != 'false') else "" for index, value in enumerate(x)], axis = 0, subset=['VM_Snapshot']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value != 0 and df_vms.at[index, 'VM_LatencySensitivity'] == 'normal' and df_vms.at[index, 'SRIOV_vNICs'] == 0 and df_vms.at[index, 'PCIPT_vNICs'] == 0) else "" for index, value in enumerate(x)], axis = 0, subset=['RAM_Reservation_GB']) # Red background if any RAM reservation and VM is not LS or has not SRIOV/PCIPT vNICs
                                .apply(lambda x: ["background-color: red" if (value != df_vms.at[index, 'VM_vMEM_GB'] and df_vms.at[index, 'VM_LatencySensitivity'] == 'high') or (value != df_vms.at[index, 'VM_vMEM_GB'] and df_vms.at[index, 'SRIOV_vNICs'] != 0) or (value != df_vms.at[index, 'VM_vMEM_GB'] and df_vms.at[index, 'PCIPT_vNICs'] != 0) else "" for index, value in enumerate(x)], axis = 0, subset=['RAM_Reservation_GB']) # Red background if not full RAM Reservation and VM is LS or has SRIOV/PCIPT vNICs
                                .apply(lambda x: ["background-color: red" if (value != 0 and df_vms.at[index, 'VM_LatencySensitivity'] == 'normal') else "" for index, value in enumerate(x)], axis = 0, subset=['CPU_Reservation_MHz']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value != (df_vms.at[index, 'VM_vCPU']*df_vms.at[index, 'Host_CPU_Package_MHz']) and df_vms.at[index, 'VM_LatencySensitivity'] == 'high') else "" for index, value in enumerate(x)], axis = 0, subset=['CPU_Reservation_MHz']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value != df_vms.at[index, 'VM_vCPU'] and df_vms.at[index, 'VM_NumaNode'] != '') or (df_vms.at[index, 'VM_vCPU']%2>0 and value != df_vms.at[index, 'VM_vCPU'] and df_vms.at[index, 'VM_NumaNode'] == '' ) or (df_vms.at[index, 'VM_vCPU']%2==0 and value != df_vms.at[index, 'VM_vCPU']/2 and df_vms.at[index, 'VM_NumaNode'] == '') else "" for index, value in enumerate(x)], axis = 0, subset=['VM_CoresPerSocket']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (not re.match('VM_[A-Z]{4}[1-9]{1}_[A-Z]{5}_[A-Z0-9.-]{1,16}_[0-9]{2}',value)) else "" for index, value in enumerate(x)], axis = 0, subset=['VM_Name']) # Red background for VMs with an incorrect VM naming
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML

        html_vms = addSortFunctionJs(html_vms, html_tableID)

        dataframe_vm_file = queryObject + '_' + queryName + '_VMs_' + time_suffix
        with open(dataframe_vm_file + '.html', 'w') as file:   # Write resulting HTML code to file
            file.write(html_vms)

        df_vms.to_csv(dataframe_vm_file + '.csv', index=False)   # Write output Dataframe to CSV file
        print()
        print(f'{dataframe_vm_file} .CSV and .HTML files saved in current directory.')

    df_hosts.fillna('',inplace=True)
    if not df_hosts.empty:
        df_hosts = df_hosts.infer_objects() # Automatically convert each DF column to the appropiate type
        html_hosts = (df_hosts.style.hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', '#4CAF50'),('color', 'white'),('padding', '5px'),('font-size', '10pt'), ('cursor', 'pointer')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'yellow')]},
                                                    {'selector': 'tr', 'props': [('font-size', '10pt')]}
                                                    ])   
                                .set_properties(**{'text-align': 'right', 'border-color': 'black', 'border-style': 'solid', 'border-width': '1px', 'font-size': '10pt'})  # Set some table properties
                                .apply(lambda x: ["background-color: silver" for index, value in enumerate(x)], axis = 0, subset=['Host_Name'])
                                .bar(subset=['Provisioned_vCPUs'], color='#08D8C3')
                                .bar(subset=['Provisioned_RAM'], color='lightgreen')
                                .bar(subset=['Datastore_Provisioned_GB'], color='#0855D8')
                                .bar(subset=['RealTime_vCPUs'], color='#FFD700')
                                .bar(subset=['SRIOV_VMs'], color='#D1D86C')
                                .bar(subset=['Datastore_MixedSpace_GB'], color='lime')
                                .background_gradient(subset=['RealTime_Occupation_%'], cmap='Greys')    # Matplotlib colormaps "https://matplotlib.org/examples/color/colormaps_reference.html"
                                .background_gradient(subset=['Total_CPU_Occupation_%'], cmap='Blues')
                                .background_gradient(subset=['Total_RAM_Occupation_%'], cmap='Greens')
                                .background_gradient(subset=['Socket0_CPU_Occupation_%'], cmap='Blues')
                                .background_gradient(subset=['Socket1_CPU_Occupation_%'], cmap='Blues')
                                .background_gradient(subset=['Socket0_RAM_Occupation_%'], cmap='Greens')
                                .background_gradient(subset=['Socket1_RAM_Occupation_%'], cmap='Greens')
                                .apply(lambda x: ["background-color: red" if (value > df_hosts.at[index, 'Max_RealTime_Occupation_%']) else "" for index, value in enumerate(x)], axis = 0, subset=['RealTime_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > df_hosts.at[index, 'Max_OverProv_Ratio_%']) else "" for index, value in enumerate(x)], axis = 0, subset=['Total_CPU_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket0_CPU_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket1_CPU_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Total_RAM_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket0_RAM_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket1_RAM_Occupation_%']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML

        html_hosts = addSortFunctionJs(html_hosts, html_tableID)

        dataframe_host_file = queryObject + '_' + queryName + '_Hosts_' + time_suffix
        with open(dataframe_host_file + '.html', 'w') as file: # Write resulting HTML code to file
            file.write(html_hosts)

        df_hosts.to_csv(dataframe_host_file + '.csv', index=False)   # Write output Dataframe to CSV file

        print()
        print(f'{dataframe_host_file} .CSV and .HTML files saved in current directory.')

        #print(df_hosts.to_markdown())
        #print(df_hosts[['Host_Name', 'Host_RealTime_vCPUs_Occupation_%', 'Host_Max_RealTime_vCPUs_%', 'Host_vCPU_Occupation_% (Prov + Hyp)', 'Host_Max_vCPU_Occupation_%']].to_markdown())

    df_hosts_network.fillna('',inplace=True)
    if not df_hosts_network.empty:
        html_hosts_network = (df_hosts_network.style
                                .hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', '#4CAF50'),('color', 'white'),('padding', '5px'),('font-size', '10pt'), ('cursor', 'pointer')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'yellow')]},
                                                    {'selector': 'tr', 'props': [('font-size', '10pt')]}
                                                    ])   
                                .set_properties(**{'text-align': 'right', 'border-color': 'black', 'border-style': 'solid', 'border-width': '1px', 'font-size': '10pt'})  # Set some table properties
                                .apply(lambda x: ["background-color: silver" for index, value in enumerate(x)], axis = 0, subset=['Host_Name'])
                                .apply(lambda x: ["background-color: lightblue" if (value == "dVS") else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Type'])
                                .apply(lambda x: ["background-color: lightgreen" if (value == "SR-IOV") else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Type'])
                                .apply(lambda x: ["background-color: orange" if (value == "PCI-PT") else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Type']) 
                                .apply(lambda x: ["background-color: red" if (value and (value != 0) and (df_hosts_network.at[index, 'vmnic_Type'] != 'SR-IOV')) else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_configured_VFs'])
                                .apply(lambda x: ["background-color: red" if value != df_hosts_network.at[index, 'Host_calculated_VF_Vector'] else "" for index, value in enumerate(x)], axis = 0, subset=['Host_current_VF_Vector'])
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML
        
        html_hosts_network = addSortFunctionJs(html_hosts_network, html_tableID)

        dataframe_host_network_file = queryObject + '_' + queryName + '_Hosts_Network_' + time_suffix
        with open(dataframe_host_network_file + '.html', 'w') as file: # Write resulting HTML code to file
            file.write(html_hosts_network)

        df_hosts_network.to_csv(dataframe_host_network_file + '.csv', index=False)   # Write output Dataframe to CSV file

        print()
        print(f'{dataframe_host_network_file} .CSV and .HTML files saved in current directory.')
        #print(df_hosts_network.to_markdown())

    if not df_clusters.empty:
        html_cluster = ""
        # Code to write Cluster output DF goes here

    if not df_datacenters.empty:
        html_datacenter = ""
        # Code to write Datacenter output DF goes here

    print()

def main():
    """Main function."""

    start_time = time.time()
    atexit.register(lambda: print("Execution time {:.2f} seconds".format(float(time.time()-start_time))))

    args = parse_arguments()
    vcenter_password = getpass.getpass(prompt='Enter vCenter password: ')

    esxi_username = ''
    esxi_password = ''
    if args.esxiuser:
        esxi_username = args.esxiuser
        esxi_password = getpass.getpass(prompt='Enter ESXi password: ')

    si = connect(args.vcenter_ip, args.vcenter_user, vcenter_password)  # Connect to vCenter
    atexit.register(Disconnect, si)     # Cleanup. Disconnect the session upon normal script termination
    content = si.RetrieveContent()

    pd.set_option('display.max_rows', None) # So that all Dataframe rows are printed to terminal

    df_vms = pd.DataFrame()
    df_hosts = pd.DataFrame()
    df_hosts_network = pd.DataFrame()
    df_clusters = pd.DataFrame()
    df_datacenters = pd.DataFrame()

    if args.t == 'vm':
        vm_obj = findVMObj(args.n, content)
        df_vms = vm_scavenger(vm_obj)
    elif args.t == 'host':
        host_obj = findHostObj(args.n, content)
        df_vms, df_hosts, df_hosts_network = host_scavenger(host_obj, args.gsw, esxi_username, esxi_password)
    elif args.t == 'cluster':
        cluster_obj = findClusterObj(args.n, content)
        df_vms, df_hosts, df_hosts_network, df_clusters = cluster_scavenger(cluster_obj, args.gsw, esxi_username, esxi_password)
    elif args.t == 'datacenter':
        datacenter_obj_list = findDatacenterObj(args.n, content)
        for datacenter_obj in datacenter_obj_list:
            df_temp_vms, df_temp_hosts, df_temp_hosts_network, df_temp_clusters, df_temp_datacenters = datacenter_scavenger(datacenter_obj, args.gsw, esxi_username, esxi_password)
            df_vms = df_vms.append(df_temp_vms, ignore_index=True)
            df_hosts = df_hosts.append(df_temp_hosts, ignore_index=True)
            df_hosts_network = df_hosts_network.append(df_temp_hosts_network, ignore_index=True)
            df_clusters = df_clusters.append(df_temp_clusters, ignore_index=True)
            df_datacenters = df_datacenters.append(df_temp_datacenters, ignore_index=True)

    writeOuputDataframes(args.t, args.n, df_vms, df_hosts, df_hosts_network, df_clusters, df_datacenters) # Print output DFs

if __name__ == '__main__':
    main()